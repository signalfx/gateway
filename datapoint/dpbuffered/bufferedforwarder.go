package dpbuffered

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/datapoint/dpsink"
	"golang.org/x/net/context"
)

// Config controls BufferedForwarder limits
type Config struct {
	BufferSize         int64
	MaxTotalDatapoints int64
	MaxDrainSize       int64
	NumDrainingThreads int64
}

// FromConfig loads the default config for a buffered forwarder from the proxy's config
func (c *Config) FromConfig(conf *config.ForwardTo) *Config {
	c.BufferSize = int64(*conf.BufferSize + 1)
	c.MaxTotalDatapoints = int64(*conf.BufferSize)

	c.NumDrainingThreads = int64(*conf.DrainingThreads)
	c.MaxDrainSize = int64(*conf.MaxDrainSize)
	return c
}

type stats struct {
	totalDatapointsBuffered int64
}

// BufferedForwarder abstracts out datapoint buffering.  Points put on its channel are buffered
// and sent in large groups to a waiting sink
type BufferedForwarder struct {
	dpChan                 (chan []*datapoint.Datapoint)
	config                 Config
	stats                  stats
	threadsWaitingToDie    sync.WaitGroup
	blockingDrainWaitMutex sync.Mutex

	sendTo      dpsink.Sink
	stopContext context.Context
	stopFunc    context.CancelFunc
}

var _ dpsink.Sink = &BufferedForwarder{}

// ErrBufferFull is returned by BufferedForwarder.AddDatapoints if the sink's buffer is full
var ErrBufferFull = errors.New("unable to send more datapoints.  Buffer full")

// AddDatapoints sends the datapoints to a chan buffer that eventually is flushed in big groups
func (forwarder *BufferedForwarder) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	atomic.AddInt64(&forwarder.stats.totalDatapointsBuffered, int64(len(points)))
	if forwarder.config.MaxTotalDatapoints <= atomic.LoadInt64(&forwarder.stats.totalDatapointsBuffered) {
		atomic.AddInt64(&forwarder.stats.totalDatapointsBuffered, int64(-len(points)))
		return ErrBufferFull
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

// Stats related to this forwarder, including errors processing datapoints
func (forwarder *BufferedForwarder) Stats(dimensions map[string]string) []*datapoint.Datapoint {
	ret := make([]*datapoint.Datapoint, 0, 2)
	ret = append(ret, datapoint.NewOnHostDatapointDimensions(
		"datapoint_chan_backup_size",
		datapoint.NewIntValue(int64(len(forwarder.dpChan))),
		datapoint.Gauge,
		dimensions))
	ret = append(ret, datapoint.NewOnHostDatapointDimensions(
		"datapoint_backup_size",
		datapoint.NewIntValue(atomic.LoadInt64(&forwarder.stats.totalDatapointsBuffered)),
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
				forwarder.sendTo.AddDatapoints(forwarder.stopContext, datapoints)
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
		config:      config,
		sendTo:      sendTo,
	}
	ret.start()
	return ret
}
