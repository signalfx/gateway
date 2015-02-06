package datapoint

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
)

// ProcessingFunction is a function that can process datapoints for the basic buffered forwarder
type ProcessingFunction func([]Datapoint) error

// BufferedForwarder abstracts out datapoint buffering.  Points put on its channel are buffered
// and sent en-masse to a ProcessingFunction inside the call to Start()
type BufferedForwarder struct {
	DatapointsChannel      (chan Datapoint)
	maxDrainSize           uint32
	name                   string
	numDrainingThreads     uint32
	started                bool
	stopped                int32
	threadsWaitingToDie    sync.WaitGroup
	blockingDrainStopChan  chan bool
	blockingDrainWaitMutex sync.Mutex

	totalProcessErrors int64
	totalDatapoints    int64
	totalProcessCalls  int64
	processErrorPoints int64
	totalProcessTimeNs int64
	callsInFlight      int64
}

// Channel to send datapoints for this forwarder.  Objects sent here will eventually pass
// to ProcessingFunction
func (forwarder *BufferedForwarder) Channel() chan<- Datapoint {
	return forwarder.DatapointsChannel
}

// Stats related to this forwarder, including errors processing datapoints
func (forwarder *BufferedForwarder) Stats() []Datapoint {
	ret := make([]Datapoint, 0, 2)
	ret = append(ret, NewOnHostDatapointDimensions(
		"datapoint_backup_size",
		NewIntValue(int64(len(forwarder.DatapointsChannel))),
		com_signalfuse_metrics_protobuf.MetricType_GAUGE,
		map[string]string{"forwarder": forwarder.Name()}))

	ret = append(ret, NewOnHostDatapointDimensions(
		"total_process_errors",
		NewIntValue(atomic.LoadInt64(&forwarder.totalProcessErrors)),
		com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		map[string]string{"forwarder": forwarder.Name()}))

	ret = append(ret, NewOnHostDatapointDimensions(
		"total_datapoints",
		NewIntValue(atomic.LoadInt64(&forwarder.totalDatapoints)),
		com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		map[string]string{"forwarder": forwarder.Name()}))

	ret = append(ret, NewOnHostDatapointDimensions(
		"total_process_calls",
		NewIntValue(atomic.LoadInt64(&forwarder.totalProcessCalls)),
		com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		map[string]string{"forwarder": forwarder.Name()}))

	ret = append(ret, NewOnHostDatapointDimensions(
		"process_error_datapoints",
		NewIntValue(atomic.LoadInt64(&forwarder.processErrorPoints)),
		com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		map[string]string{"forwarder": forwarder.Name()}))

	ret = append(ret, NewOnHostDatapointDimensions(
		"process_time_ns",
		NewIntValue(atomic.LoadInt64(&forwarder.totalProcessTimeNs)),
		com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		map[string]string{"forwarder": forwarder.Name()}))

	ret = append(ret, NewOnHostDatapointDimensions(
		"calls_in_flight",
		NewIntValue(atomic.LoadInt64(&forwarder.callsInFlight)),
		com_signalfuse_metrics_protobuf.MetricType_GAUGE,
		map[string]string{"forwarder": forwarder.Name()}))
	return ret
}

func (forwarder *BufferedForwarder) blockingDrainUpTo() []Datapoint {
	// We block the mutex so we allow one drainer to fully drain until we use another
	forwarder.blockingDrainWaitMutex.Lock()
	defer forwarder.blockingDrainWaitMutex.Unlock()

	datapoints := make([]Datapoint, 0, forwarder.maxDrainSize)

	// Block for at least one point
	select {
	case datapoint := <-forwarder.DatapointsChannel:
		datapoints = append(datapoints, datapoint)
		break
	case _ = <-forwarder.blockingDrainStopChan:
		forwarder.blockingDrainStopChan <- true
		return datapoints
	}
Loop:
	for uint32(len(datapoints)) < forwarder.maxDrainSize {
		select {
		case datapoint := <-forwarder.DatapointsChannel:
			datapoints = append(datapoints, datapoint)
			continue
		default:
			// Nothing left.  Flush this.
			break Loop
		}
	}
	return datapoints
}

func (forwarder *BufferedForwarder) stop() {
	// Set stop flag
	atomic.StoreInt32(&forwarder.stopped, 1)
	select {
	case forwarder.blockingDrainStopChan <- true:
	default:
	}
	forwarder.threadsWaitingToDie.Wait()
}

var errForwarderStarted = errors.New("Forwarder already started")

// Start draining points from this forwarder.  Points are sent to processor across multiple
// threads, in bulk.
func (forwarder *BufferedForwarder) Start(processor ProcessingFunction) error {
	if forwarder.started {
		return errForwarderStarted
	}
	forwarder.started = true
	for i := uint32(0); i < forwarder.numDrainingThreads; i++ {
		go func() {
			forwarder.threadsWaitingToDie.Add(1)
			defer forwarder.threadsWaitingToDie.Done()
			for atomic.LoadInt32(&forwarder.stopped) == 0 {
				datapoints := forwarder.blockingDrainUpTo()
				if len(datapoints) == 0 {
					continue
				}
				atomic.AddInt64(&forwarder.totalDatapoints, int64(len(datapoints)))
				atomic.AddInt64(&forwarder.totalProcessCalls, 1)
				atomic.AddInt64(&forwarder.callsInFlight, 1)
				start := time.Now()
				err := processor(datapoints)
				atomic.AddInt64(&forwarder.totalProcessTimeNs, time.Since(start).Nanoseconds())
				atomic.AddInt64(&forwarder.callsInFlight, -1)
				if err != nil {
					atomic.AddInt64(&forwarder.totalProcessErrors, 1)
					atomic.AddInt64(&forwarder.processErrorPoints, int64(len(datapoints)))
					log.WithField("err", err).Warn("Unable to process datapoints")
					continue
				}
			}
		}()
	}
	return nil
}

// Name given to this forwarder
func (forwarder *BufferedForwarder) Name() string {
	return forwarder.name
}

// NewBufferedForwarder is used only by this package to create a forwarder that buffers its
// datapoint channel
func NewBufferedForwarder(bufferSize uint32, maxDrainSize uint32, name string, numDrainingThreads uint32) *BufferedForwarder {
	return &BufferedForwarder{
		DatapointsChannel:     make(chan Datapoint, bufferSize),
		maxDrainSize:          maxDrainSize,
		name:                  name,
		numDrainingThreads:    numDrainingThreads,
		started:               false,
		blockingDrainStopChan: make(chan bool, 2),
	}
}
