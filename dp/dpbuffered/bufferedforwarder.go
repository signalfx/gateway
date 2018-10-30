package dpbuffered

import (
	"sync"
	"sync/atomic"

	"context"
	"fmt"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/trace"
	"github.com/signalfx/metricproxy/logkey"
	"github.com/signalfx/metricproxy/protocol/signalfx"
)

// Config controls BufferedForwarder limits
type Config struct {
	BufferSize         *int64
	MaxTotalDatapoints *int64
	MaxTotalEvents     *int64
	MaxTotalSpans      *int64
	MaxDrainSize       *int64
	NumDrainingThreads *int64
	Checker            *dpsink.ItemFlagger
	Cdim               *log.CtxDimensions
	Name               *string
}

func (c *Config) String() string {
	return fmt.Sprintf("Config [BufferSize: %d MaxTotalDatapoints: %d MaxTotalEvents: %d MaxTotalSpans: %d MaxDrainSize: %d NumDrainingThreads: %d", *c.BufferSize, *c.MaxTotalDatapoints, *c.MaxTotalEvents, *c.MaxTotalSpans, *c.MaxDrainSize, *c.NumDrainingThreads)
}

// DefaultConfig are default values for buffered forwarders
var DefaultConfig = &Config{
	BufferSize:         pointer.Int64(1000000),
	MaxTotalDatapoints: pointer.Int64(1000000),
	MaxTotalEvents:     pointer.Int64(1000000),
	MaxTotalSpans:      pointer.Int64(1000000),
	MaxDrainSize:       pointer.Int64(30000),
	NumDrainingThreads: pointer.Int64(10),
	Name:               pointer.String(""),
}

type stats struct {
	totalDatapointsBuffered int64
	totalEventsBuffered     int64
	datapointsInFlight      int64
	eventsInFlight          int64
	totalTracesBuffered     int64
	tracesInFlight          int64
}

// BufferedForwarder abstracts out datapoint buffering.  Points put on its channel are buffered
// and sent in large groups to a waiting sink
type BufferedForwarder struct {
	dpChan                      chan []*datapoint.Datapoint
	eChan                       chan []*event.Event
	tChan                       chan []*trace.Span
	config                      *Config
	stats                       stats
	threadsWaitingToDie         sync.WaitGroup
	blockingDrainWaitMutex      sync.Mutex
	blockingEventDrainWaitMutex sync.Mutex
	blockingTraceDrainWaitMutex sync.Mutex
	logger                      log.Logger
	checker                     *dpsink.ItemFlagger
	cdim                        *log.CtxDimensions
	identifier                  string

	sendTo      signalfx.Sink
	closeSender func() error
	stopContext context.Context
	stopFunc    context.CancelFunc
}

var _ dpsink.Sink = &BufferedForwarder{}

type errDPBufferFull string

func (e errDPBufferFull) Error() string {
	return "Forwarder " + string(e) + " unable to send more datapoints.  Buffer full"
}

// AddDatapoints sends the datapoints to a chan buffer that eventually is flushed in big groups
func (forwarder *BufferedForwarder) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	if forwarder.checker.CtxFlagCheck.HasFlag(ctx) {
		forwarder.cdim.With(ctx, forwarder.logger).Log("Datapoint call recieved in buffered forwarder")
	}
	atomic.AddInt64(&forwarder.stats.totalDatapointsBuffered, int64(len(points)))
	if *forwarder.config.MaxTotalDatapoints <= atomic.LoadInt64(&forwarder.stats.totalDatapointsBuffered) {
		atomic.AddInt64(&forwarder.stats.totalDatapointsBuffered, int64(-len(points)))
		return errDPBufferFull(forwarder.identifier)
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

type errEBufferFull string

func (e errEBufferFull) Error() string {
	return "Forwarder " + string(e) + " unable to send more events.  Buffer full"
}

// AddEvents sends the events to a chan buffer that eventually is flushed in big groups
func (forwarder *BufferedForwarder) AddEvents(ctx context.Context, events []*event.Event) error {
	if forwarder.checker.CtxFlagCheck.HasFlag(ctx) {
		forwarder.cdim.With(ctx, forwarder.logger).Log("Events call received in buffered forwarder")
	}
	atomic.AddInt64(&forwarder.stats.totalEventsBuffered, int64(len(events)))
	if *forwarder.config.MaxTotalEvents <= atomic.LoadInt64(&forwarder.stats.totalEventsBuffered) {
		atomic.AddInt64(&forwarder.stats.totalEventsBuffered, int64(-len(events)))
		return errEBufferFull(forwarder.identifier)
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

type errTBufferFull string

func (e errTBufferFull) Error() string {
	return "Forwarder " + string(e) + " unable to send more traces.  Buffer full"
}

// AddSpans sends the traces to a chan buffer that traceually is flushed in big groups
func (forwarder *BufferedForwarder) AddSpans(ctx context.Context, traces []*trace.Span) error {
	atomic.AddInt64(&forwarder.stats.totalTracesBuffered, int64(len(traces)))
	if *forwarder.config.MaxTotalSpans <= atomic.LoadInt64(&forwarder.stats.totalTracesBuffered) {
		atomic.AddInt64(&forwarder.stats.totalTracesBuffered, int64(-len(traces)))
		return errTBufferFull(forwarder.identifier)
	}
	select {
	case forwarder.tChan <- traces:
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
		sfxclient.Gauge("trace_chan_backup_size", nil, int64(len(forwarder.tChan))),
		sfxclient.Gauge("trace_backup_size", nil, atomic.LoadInt64(&forwarder.stats.totalTracesBuffered)),
	}
}

// Pipeline for a BufferedForwarder is the total of all buffers and what is in flight
func (forwarder *BufferedForwarder) Pipeline() int64 {
	return int64(len(forwarder.dpChan)) + int64(len(forwarder.eChan)) + atomic.LoadInt64(&forwarder.stats.datapointsInFlight) + atomic.LoadInt64(&forwarder.stats.eventsInFlight) + int64(len(forwarder.tChan)) + atomic.LoadInt64(&forwarder.stats.tracesInFlight)
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

func (forwarder *BufferedForwarder) blockingDrainSpansUpTo() []*trace.Span {
	// We block the mutex so we allow one drainer to fully drain until we use another
	forwarder.blockingTraceDrainWaitMutex.Lock()
	defer forwarder.blockingTraceDrainWaitMutex.Unlock()

	// Block for at least one trace
	select {
	case traces := <-forwarder.tChan:
	Loop:
		for int64(len(traces)) < *forwarder.config.MaxDrainSize {
			select {
			case trace := <-forwarder.tChan:
				traces = append(traces, trace...)
				continue
			default:
				// Nothing left.  Flush this.
				break Loop
			}
		}
		atomic.AddInt64(&forwarder.stats.totalTracesBuffered, int64(-len(traces)))
		return traces
	case <-forwarder.stopContext.Done():
		return []*trace.Span{}
	}
}

// Close stops the threads that are flushing channel points to the next forwarder
func (forwarder *BufferedForwarder) Close() error {
	forwarder.stopFunc()
	forwarder.threadsWaitingToDie.Wait()
	return forwarder.closeSender()
}

func (forwarder *BufferedForwarder) doData(drainIndex int64) {
	defer forwarder.threadsWaitingToDie.Done()
	logger := log.NewContext(forwarder.logger).With("draining_index", drainIndex)
	for forwarder.stopContext.Err() == nil {
		datapoints := forwarder.blockingDrainUpTo()
		if len(datapoints) == 0 {
			continue
		}
		logDpIfFlag(logger, forwarder.checker, datapoints, "about to send datapoint")
		atomic.AddInt64(&forwarder.stats.datapointsInFlight, int64(len(datapoints)))
		err := forwarder.sendTo.AddDatapoints(forwarder.stopContext, datapoints)
		atomic.AddInt64(&forwarder.stats.datapointsInFlight, -int64(len(datapoints)))
		if err != nil {
			logger.Log(log.Err, err, "error sending datapoints")
		}
		logDpIfFlag(logger, forwarder.checker, datapoints, "Finished sending datapoint")
	}
}

func (forwarder *BufferedForwarder) doEvent(drainIndex int64) {
	defer forwarder.threadsWaitingToDie.Done()
	logger := log.NewContext(forwarder.logger).With("draining_index", drainIndex)
	for forwarder.stopContext.Err() == nil {
		events := forwarder.blockingDrainEventsUpTo()
		if len(events) == 0 {
			continue
		}
		logEvIfFlag(logger, forwarder.checker, events, "about to send event")
		atomic.AddInt64(&forwarder.stats.eventsInFlight, int64(len(events)))
		err := forwarder.sendTo.AddEvents(forwarder.stopContext, events)
		atomic.AddInt64(&forwarder.stats.eventsInFlight, -int64(len(events)))
		if err != nil {
			logger.Log(log.Err, err, "error sending events")
		}
		logEvIfFlag(logger, forwarder.checker, events, "Finished sending event")
	}
}

func (forwarder *BufferedForwarder) doSpan(drainIndex int64) {
	defer forwarder.threadsWaitingToDie.Done()
	logger := log.NewContext(forwarder.logger).With("draining_index", drainIndex)
	for forwarder.stopContext.Err() == nil {
		traces := forwarder.blockingDrainSpansUpTo()
		if len(traces) == 0 {
			continue
		}
		atomic.AddInt64(&forwarder.stats.tracesInFlight, int64(len(traces)))
		err := forwarder.sendTo.AddSpans(forwarder.stopContext, traces)
		atomic.AddInt64(&forwarder.stats.tracesInFlight, -int64(len(traces)))
		if err != nil {
			logger.Log(log.Err, err, "error sending traces")
		}
	}
}

func (forwarder *BufferedForwarder) start() {
	forwarder.threadsWaitingToDie.Add(int(*forwarder.config.NumDrainingThreads) * 3)
	for i := int64(0); i < *forwarder.config.NumDrainingThreads; i++ {
		go forwarder.doData(i)
		go forwarder.doEvent(i)
		go forwarder.doSpan(i)
	}
}

type flagChecker interface {
	HasDatapointFlag(*datapoint.Datapoint) bool
	HasEventFlag(*event.Event) bool
}

func logDpIfFlag(l log.Logger, checker flagChecker, dps []*datapoint.Datapoint, msg string) {
	if log.IsDisabled(l) {
		return
	}
	for _, dp := range dps {
		if checker.HasDatapointFlag(dp) {
			l.Log("dp", dp, msg)
		}
	}
}

func logEvIfFlag(l log.Logger, checker flagChecker, ev []*event.Event, msg string) {
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
func NewBufferedForwarder(ctx context.Context, config *Config, sendTo signalfx.Sink, closeIt func() error, logger log.Logger) *BufferedForwarder {
	config = pointer.FillDefaultFrom(config, DefaultConfig).(*Config)
	logCtx := log.NewContext(logger).With(logkey.Struct, "BufferedForwarder")
	logCtx.Log(logkey.Config, config)
	context, cancel := context.WithCancel(ctx)
	ret := &BufferedForwarder{
		stopFunc:    cancel,
		stopContext: context,
		dpChan:      make(chan []*datapoint.Datapoint, *config.BufferSize),
		eChan:       make(chan []*event.Event, *config.BufferSize),
		tChan:       make(chan []*trace.Span, *config.BufferSize),
		config:      config,
		sendTo:      sendTo,
		closeSender: closeIt,
		logger:      logCtx,
		checker:     config.Checker,
		cdim:        config.Cdim,
		identifier:  *config.Name,
	}
	ret.start()
	return ret
}
