package dpbuffered

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	log "github.com/Sirupsen/logrus"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dplocal"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/metricproxy/config"
	"golang.org/x/net/context"
)

// Config controls BufferedForwarder limits
type Config struct {
	BufferSize         int64
	MaxTotalDatapoints int64
	MaxTotalEvents     int64
	MaxDrainSize       int64
	NumDrainingThreads int64
}

// FromConfig loads the default config for a buffered forwarder from the proxy's config
func (c *Config) FromConfig(conf *config.ForwardTo) *Config {
	c.BufferSize = int64(*conf.BufferSize + 1)
	c.MaxTotalDatapoints = int64(*conf.BufferSize)
	c.MaxTotalEvents = int64(*conf.BufferSize)

	c.NumDrainingThreads = int64(*conf.DrainingThreads)
	c.MaxDrainSize = int64(*conf.MaxDrainSize)
	return c
}

type stats struct {
	totalDatapointsBuffered int64
	totalEventsBuffered     int64
}

// BufferedForwarder abstracts out datapoint buffering.  Points put on its channel are buffered
// and sent in large groups to a waiting sink
type BufferedForwarder struct {
	dpChan                      (chan []*datapoint.Datapoint)
	eChan                       (chan []*event.Event)
	config                      Config
	stats                       stats
	threadsWaitingToDie         sync.WaitGroup
	blockingDrainWaitMutex      sync.Mutex
	blockingEventDrainWaitMutex sync.Mutex

	sendTo      dpsink.Sink
	stopContext context.Context
	stopFunc    context.CancelFunc
}

var _ dpsink.Sink = &BufferedForwarder{}

// ErrDPBufferFull is returned by BufferedForwarder.AddDatapoints if the sink's buffer is full
var ErrDPBufferFull = errors.New("unable to send more datapoints.  Buffer full")

// AddDatapoints sends the datapoints to a chan buffer that eventually is flushed in big groups
func (forwarder *BufferedForwarder) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	atomic.AddInt64(&forwarder.stats.totalDatapointsBuffered, int64(len(points)))
	if forwarder.config.MaxTotalDatapoints <= atomic.LoadInt64(&forwarder.stats.totalDatapointsBuffered) {
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
	atomic.AddInt64(&forwarder.stats.totalEventsBuffered, int64(len(events)))
	if forwarder.config.MaxTotalEvents <= atomic.LoadInt64(&forwarder.stats.totalEventsBuffered) {
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

// Stats related to this forwarder, including errors processing datapoints
func (forwarder *BufferedForwarder) Stats(dimensions map[string]string) []*datapoint.Datapoint {
	ret := make([]*datapoint.Datapoint, 0, 2)
	ret = append(ret, dplocal.NewOnHostDatapointDimensions(
		"datapoint_chan_backup_size",
		datapoint.NewIntValue(int64(len(forwarder.dpChan))),
		datapoint.Gauge,
		dimensions))
	ret = append(ret, dplocal.NewOnHostDatapointDimensions(
		"event_chan_backup_size",
		datapoint.NewIntValue(int64(len(forwarder.eChan))),
		datapoint.Gauge,
		dimensions))
	ret = append(ret, dplocal.NewOnHostDatapointDimensions(
		"datapoint_backup_size",
		datapoint.NewIntValue(atomic.LoadInt64(&forwarder.stats.totalDatapointsBuffered)),
		datapoint.Gauge,
		dimensions))
	ret = append(ret, dplocal.NewOnHostDatapointDimensions(
		"event_backup_size",
		datapoint.NewIntValue(atomic.LoadInt64(&forwarder.stats.totalEventsBuffered)),
		datapoint.Gauge,
		dimensions))
	return ret
}

func (forwarder *BufferedForwarder) blockingDrainUpTo() []*datapoint.Datapoint {
	// We block the mutex so we allow one drainer to fully drain until we use another
	forwarder.blockingDrainWaitMutex.Lock()
	defer forwarder.blockingDrainWaitMutex.Unlock()

	// Block for at least one point
	select {
	case datapoints := <-forwarder.dpChan:
	Loop:
		for int64(len(datapoints)) < forwarder.config.MaxDrainSize {
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
		for int64(len(events)) < forwarder.config.MaxDrainSize {
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
func (forwarder *BufferedForwarder) Close() {
	forwarder.stopFunc()
	forwarder.threadsWaitingToDie.Wait()
}

func (forwarder *BufferedForwarder) start() {
	for i := int64(0); i < forwarder.config.NumDrainingThreads; i++ {
		forwarder.threadsWaitingToDie.Add(1)
		go func() {
			defer forwarder.threadsWaitingToDie.Done()
			for forwarder.stopContext.Err() == nil {
				datapoints := forwarder.blockingDrainUpTo()
				if len(datapoints) == 0 {
					continue
				}
				err := forwarder.sendTo.AddDatapoints(forwarder.stopContext, datapoints)
				if err != nil {
					log.Error(fmt.Sprintf("Error sending datapoints %v", err))
				}
			}
		}()
		forwarder.threadsWaitingToDie.Add(1)
		go func() {
			defer forwarder.threadsWaitingToDie.Done()
			for forwarder.stopContext.Err() == nil {
				events := forwarder.blockingDrainEventsUpTo()
				if len(events) == 0 {
					continue
				}
				err := forwarder.sendTo.AddEvents(forwarder.stopContext, events)
				if err != nil {
					log.Error(fmt.Sprintf("Error sending events %v", err))
				}
			}
		}()
	}
}

// NewBufferedForwarder is used only by this package to create a forwarder that buffers its
// datapoint channel
func NewBufferedForwarder(ctx context.Context, config Config, sendTo dpsink.Sink) *BufferedForwarder {
	context, cancel := context.WithCancel(ctx)
	ret := &BufferedForwarder{
		stopFunc:    cancel,
		stopContext: context,
		dpChan:      make(chan []*datapoint.Datapoint, config.BufferSize),
		eChan:       make(chan []*event.Event, config.BufferSize),
		config:      config,
		sendTo:      sendTo,
	}
	ret.start()
	return ret
}
