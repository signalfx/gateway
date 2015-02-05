package forwarder

import (
	"errors"
	"sync"
	"sync/atomic"

	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
)

// ProcessingFunction is a function that can process datapoints for the basic buffered forwarder
type ProcessingFunction func([]core.Datapoint) error

type basicBufferedForwarder struct {
	datapointsChannel      (chan core.Datapoint)
	bufferSize             uint32
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

// Loader is the function definition of a function that can load a config
// for a proxy and return the streamer
type Loader func(*config.ForwardTo) (core.StatKeepingStreamingAPI, error)

func (forwarder *basicBufferedForwarder) DatapointsChannel() chan<- core.Datapoint {
	return forwarder.datapointsChannel
}
func (forwarder *basicBufferedForwarder) GetStats() []core.Datapoint {
	ret := make([]core.Datapoint, 0, 2)
	ret = append(ret, protocoltypes.NewOnHostDatapointDimensions(
		"datapoint_backup_size",
		value.NewIntWire(int64(len(forwarder.datapointsChannel))),
		com_signalfuse_metrics_protobuf.MetricType_GAUGE,
		map[string]string{"forwarder": forwarder.Name()}))

	ret = append(ret, protocoltypes.NewOnHostDatapointDimensions(
		"total_process_errors",
		value.NewIntWire(atomic.LoadInt64(&forwarder.totalProcessErrors)),
		com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		map[string]string{"forwarder": forwarder.Name()}))

	ret = append(ret, protocoltypes.NewOnHostDatapointDimensions(
		"total_datapoints",
		value.NewIntWire(atomic.LoadInt64(&forwarder.totalDatapoints)),
		com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		map[string]string{"forwarder": forwarder.Name()}))

	ret = append(ret, protocoltypes.NewOnHostDatapointDimensions(
		"total_process_calls",
		value.NewIntWire(atomic.LoadInt64(&forwarder.totalProcessCalls)),
		com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		map[string]string{"forwarder": forwarder.Name()}))

	ret = append(ret, protocoltypes.NewOnHostDatapointDimensions(
		"process_error_datapoints",
		value.NewIntWire(atomic.LoadInt64(&forwarder.processErrorPoints)),
		com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		map[string]string{"forwarder": forwarder.Name()}))

	ret = append(ret, protocoltypes.NewOnHostDatapointDimensions(
		"process_time_ns",
		value.NewIntWire(atomic.LoadInt64(&forwarder.totalProcessTimeNs)),
		com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		map[string]string{"forwarder": forwarder.Name()}))

	ret = append(ret, protocoltypes.NewOnHostDatapointDimensions(
		"calls_in_flight",
		value.NewIntWire(atomic.LoadInt64(&forwarder.callsInFlight)),
		com_signalfuse_metrics_protobuf.MetricType_GAUGE,
		map[string]string{"forwarder": forwarder.Name()}))
	return ret
}

func (forwarder *basicBufferedForwarder) blockingDrainUpTo() []core.Datapoint {
	// We block the mutex so we allow one drainer to fully drain until we use another
	forwarder.blockingDrainWaitMutex.Lock()
	defer forwarder.blockingDrainWaitMutex.Unlock()

	datapoints := make([]core.Datapoint, 0, forwarder.maxDrainSize)

	// Block for at least one point
	select {
	case datapoint := <-forwarder.datapointsChannel:
		datapoints = append(datapoints, datapoint)
		break
	case _ = <-forwarder.blockingDrainStopChan:
		forwarder.blockingDrainStopChan <- true
		return datapoints
	}
Loop:
	for uint32(len(datapoints)) < forwarder.maxDrainSize {
		select {
		case datapoint := <-forwarder.datapointsChannel:
			datapoints = append(datapoints, datapoint)
			continue
		default:
			log.WithField("len", len(forwarder.datapointsChannel)).Debug("Nothing on channel")
			// Nothing left.  Flush this.
			break Loop
		}
	}
	return datapoints
}

func (forwarder *basicBufferedForwarder) stop() {
	// Set stop flag
	atomic.StoreInt32(&forwarder.stopped, 1)
	select {
	case forwarder.blockingDrainStopChan <- true:
	default:
	}
	forwarder.threadsWaitingToDie.Wait()
}

func (forwarder *basicBufferedForwarder) start(processor ProcessingFunction) error {
	if forwarder.started {
		return errors.New("forwarder already started")
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
				start := time.Now()
				atomic.AddInt64(&forwarder.totalDatapoints, int64(len(datapoints)))
				atomic.AddInt64(&forwarder.totalProcessCalls, 1)
				err := processor(datapoints)
				atomic.AddInt64(&forwarder.totalProcessTimeNs, time.Since(start).Nanoseconds())
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

func (forwarder *basicBufferedForwarder) Name() string {
	return forwarder.name
}

// newBasicBufferedForwarder is used only by this package to create a forwarder that buffers its
// datapoint channel
func newBasicBufferedForwarder(bufferSize uint32, maxDrainSize uint32, name string, numDrainingThreads uint32) *basicBufferedForwarder {
	return &basicBufferedForwarder{
		datapointsChannel:     make(chan core.Datapoint, bufferSize),
		maxDrainSize:          maxDrainSize,
		bufferSize:            bufferSize,
		name:                  name,
		numDrainingThreads:    numDrainingThreads,
		started:               false,
		blockingDrainStopChan: make(chan bool, 2),
	}
}

// AllForwarderLoaders is a map of config names to loaders for that config
var AllForwarderLoaders = map[string]Loader{
	"signalfx-json": SignalfxJSONForwarderLoader,
	"carbon":        TcpGraphiteCarbonForwarerLoader,
	"csv":           CsvForwarderLoader,
}
