package dpbuffered

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/metricproxy/logkey"
	"golang.org/x/net/context"
)

// Config controls BufferedForwarder limits
type Config struct {
	BufferSize         *int64
	MaxTotalDatapoints *int64
	MaxTotalEvents     *int64
	MaxDrainSize       *int64
	NumDrainingThreads *int64
	Checker            *dpsink.ItemFlagger
	Cdim               *log.CtxDimensions
}

// DefaultConfig are default values for buffered forwarders
var DefaultConfig = &Config{
	BufferSize:         pointer.Int64(10000),
	MaxTotalDatapoints: pointer.Int64(10000),
	MaxTotalEvents:     pointer.Int64(10000),
	MaxDrainSize:       pointer.Int64(1000),
	NumDrainingThreads: pointer.Int64(5),
}

type stats struct {
	totalDatapointsBuffered int64
	totalEventsBuffered     int64
}

// BufferedForwarder abstracts out datapoint buffering.  Points put on its channel are buffered
// and sent in large groups to a waiting sink
type BufferedForwarder struct {
	dpChan                      chan []*datapoint.Datapoint
	eChan                       chan []*event.Event
	config                      *Config
	stats                       stats
	threadsWaitingToDie         sync.WaitGroup
	blockingDrainWaitMutex      sync.Mutex
	blockingEventDrainWaitMutex sync.Mutex
	logger                      log.Logger
	checker                     *dpsink.ItemFlagger
	cdim                        *log.CtxDimensions

	sendTo      dpsink.Sink
	stopContext context.Context
	stopFunc    context.CancelFunc
}

var _ dpsink.Sink = &BufferedForwarder{}

// ErrDPBufferFull is returned by BufferedForwarder.AddDatapoints if the sink's buffer is full
var ErrDPBufferFull = errors.New("unable to send more datapoints.  Buffer full")

// AddDatapoints sends the datapoints to a chan buffer that eventually is flushed in big groups
func (forwarder *BufferedForwarder) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	if forwarder.checker.CtxFlagCheck.HasFlag(ctx) {
		forwarder.cdim.With(ctx, forwarder.logger).Log("Datapoint call recieved in buffered forwarder")
	}
	atomic.AddInt64(&forwarder.stats.totalDatapointsBuffered, int64(len(points)))
	if *forwarder.config.MaxTotalDatapoints <= atomic.LoadInt64(&forwarder.stats.totalDatapointsBuffered) {
		atomic.AddInt64(&forwarder.stats.totalDatapointsBuffered, int64(-len(points)))
		return ErrDPBufferFull
	}
	select {
	case forwarder.dpChan <- points:
		return nil
	case <-forwarder.stopContext.Done():
		return forwarder.stopContext.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ErrEBufferFull is returned by BufferedForwarder.AddEvents if the sink's buffer is full
var ErrEBufferFull = errors.New("unable to send more events.  Buffer full")

// AddEvents sends the events to a chan buffer that eventually is flushed in big groups
func (forwarder *BufferedForwarder) AddEvents(ctx context.Context, events []*event.Event) error {
	if forwarder.checker.CtxFlagCheck.HasFlag(ctx) {
		forwarder.cdim.With(ctx, forwarder.logger).Log("Events call recieved in buffered forwarder")
	}
	atomic.AddInt64(&forwarder.stats.totalEventsBuffered, int64(len(events)))
	if *forwarder.config.MaxTotalEvents <= atomic.LoadInt64(&forwarder.stats.totalEventsBuffered) {
		atomic.AddInt64(&forwarder.stats.totalEventsBuffered, int64(-len(events)))
		return ErrEBufferFull
	}
	select {
	case forwarder.eChan <- events:
		return nil
	case <-forwarder.stopContext.Done():
		return forwarder.stopContext.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Datapoints related to this forwarder, including errors processing datapoints
func (forwarder *BufferedForwarder) Datapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.Gauge("datapoint_chan_backup_size", nil, int64(len(forwarder.dpChan))),
		sfxclient.Gauge("event_chan_backup_size", nil, int64(len(forwarder.eChan))),
		sfxclient.Gauge("datapoint_backup_size", nil, atomic.LoadInt64(&forwarder.stats.totalDatapointsBuffered)),
		sfxclient.Gauge("event_backup_size", nil, atomic.LoadInt64(&forwarder.stats.totalEventsBuffered)),
	}
}

// BufferSize for a BufferedForwarder is the total of both buffers
func (forwarder *BufferedForwarder) BufferSize() int64 {
	return int64(len(forwarder.dpChan)) + int64(len(forwarder.eChan))
}

func (forwarder *BufferedForwarder) blockingDrainUpTo() []*datapoint.Datapoint {
	// We block the mutex so we allow one drainer to fully drain until we use another
	forwarder.blockingDrainWaitMutex.Lock()
	defer forwarder.blockingDrainWaitMutex.Unlock()

	// Block for at least one point
	select {
	case datapoints := <-forwarder.dpChan:
	Loop:
		for int64(len(datapoints)) < *forwarder.config.MaxDrainSize {
			select {
			case datapoint := <-forwarder.dpChan:
				datapoints = append(datapoints, datapoint...)
				continue
			default:
				// Nothing left.  Flush this.
				break Loop
			}
		}
		atomic.AddInt64(&forwarder.stats.totalDatapointsBuffered, int64(-len(datapoints)))
		return datapoints
	case <-forwarder.stopContext.Done():
		return []*datapoint.Datapoint{}
	}
}

func (forwarder *BufferedForwarder) blockingDrainEventsUpTo() []*event.Event {
	// We block the mutex so we allow one drainer to fully drain until we use another
	forwarder.blockingEventDrainWaitMutex.Lock()
	defer forwarder.blockingEventDrainWaitMutex.Unlock()

	// Block for at least one event
	select {
	case events := <-forwarder.eChan:
	Loop:
		for int64(len(events)) < *forwarder.config.MaxDrainSize {
			select {
			case event := <-forwarder.eChan:
				events = append(events, event...)
				continue
			default:
				// Nothing left.  Flush this.
				break Loop
			}
		}
		atomic.AddInt64(&forwarder.stats.totalEventsBuffered, int64(-len(events)))
		return events
	case <-forwarder.stopContext.Done():
		return []*event.Event{}
	}
}

// Close stops the threads that are flushing channel points to the next forwarder
func (forwarder *BufferedForwarder) Close() error {
	forwarder.stopFunc()
	forwarder.threadsWaitingToDie.Wait()
	return nil
}

func (forwarder *BufferedForwarder) start() {
	forwarder.threadsWaitingToDie.Add(int(*forwarder.config.NumDrainingThreads) * 2)
	for i := int64(0); i < *forwarder.config.NumDrainingThreads; i++ {
		go func(drainIndex int64) {
			defer forwarder.threadsWaitingToDie.Done()
			logger := log.NewContext(forwarder.logger).With("draining_index", drainIndex)
			for forwarder.stopContext.Err() == nil {
				datapoints := forwarder.blockingDrainUpTo()
				if len(datapoints) == 0 {
					continue
				}
				logDpIfFlag(logger, forwarder.checker, datapoints, "about to send datapoint")
				err := forwarder.sendTo.AddDatapoints(forwarder.stopContext, datapoints)
				if err != nil {
					logger.Log(log.Err, err, "error sending datapoints")
				}
				logDpIfFlag(logger, forwarder.checker, datapoints, "Finished sending datapoint")
			}
		}(i)
		go func(drainIndex int64) {
			defer forwarder.threadsWaitingToDie.Done()
			logger := log.NewContext(forwarder.logger).With("draining_index", drainIndex)
			for forwarder.stopContext.Err() == nil {
				events := forwarder.blockingDrainEventsUpTo()
				if len(events) == 0 {
					continue
				}
				logEvIfFlag(logger, forwarder.checker, events, "about to send event")
				err := forwarder.sendTo.AddEvents(forwarder.stopContext, events)
				if err != nil {
					logger.Log(log.Err, err, "error sending events")
				}
				logEvIfFlag(logger, forwarder.checker, events, "Finished sending event")
			}
		}(i)
	}
}

func logDpIfFlag(l log.Logger, checker *dpsink.ItemFlagger, dps []*datapoint.Datapoint, msg string) {
	if log.IsDisabled(l) {
		return
	}
	for _, dp := range dps {
		if checker.HasDatapointFlag(dp) {
			l.Log("dp", dp, msg)
		}
	}
}

func logEvIfFlag(l log.Logger, checker *dpsink.ItemFlagger, ev []*event.Event, msg string) {
	if log.IsDisabled(l) {
		return
	}
	for _, dp := range ev {
		if checker.HasEventFlag(dp) {
			l.Log("ev", dp, msg)
		}
	}
}

// NewBufferedForwarder is used only by this package to create a forwarder that buffers its
// datapoint channel
func NewBufferedForwarder(ctx context.Context, config *Config, sendTo dpsink.Sink, logger log.Logger) *BufferedForwarder {
	config = pointer.FillDefaultFrom(config, DefaultConfig).(*Config)
	logCtx := log.NewContext(logger).With(logkey.Struct, "BufferedForwarder")
	logCtx.Log(logkey.Config, config)
	context, cancel := context.WithCancel(ctx)
	ret := &BufferedForwarder{
		stopFunc:    cancel,
		stopContext: context,
		dpChan:      make(chan []*datapoint.Datapoint, *config.BufferSize),
		eChan:       make(chan []*event.Event, *config.BufferSize),
		config:      config,
		sendTo:      sendTo,
		logger:      logCtx,
		checker:     config.Checker,
		cdim:        config.Cdim,
	}
	ret.start()
	return ret
}
