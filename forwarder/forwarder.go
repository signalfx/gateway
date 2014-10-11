package forwarder

import (
	"github.com/golang/glog"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"sync/atomic"
)

// ProcessingFunction is a function that can process datapoints for the basic buffered forwarder
type ProcessingFunction func([]core.Datapoint) error

type basicBufferedForwarder struct {
	datapointsChannel     (chan core.Datapoint)
	bufferSize            uint32
	maxDrainSize          uint32
	name                  string
	numDrainingThreads    uint32
	started               bool
	stopped               int32
	blockingDrainStopChan chan (chan bool)
}

// ForwarderLoader is the function definition of a function that can load a config
// for a proxy and return the streamer
type ForwarderLoader func(*config.ForwardTo) (core.StatKeepingStreamingAPI, error)

func (forwarder *basicBufferedForwarder) DatapointsChannel() (chan<- core.Datapoint) {
	return forwarder.datapointsChannel
}

func (forwarder *basicBufferedForwarder) blockingDrainUpTo() ([]core.Datapoint, error) {
	// Block for at least one point
	datapoints := []core.Datapoint{}
	select {
	case datapoint := <-forwarder.datapointsChannel:
		datapoints = append(datapoints, datapoint)
		break
	case stopFlag := <-forwarder.blockingDrainStopChan:
		stopFlag <- true
		return datapoints, nil
	}
Loop:
	for uint32(len(datapoints)) < forwarder.maxDrainSize {
		glog.V(2).Infof("Less than size: %d < %d len is %d", len(datapoints), forwarder.maxDrainSize, len(forwarder.datapointsChannel))
		select {
		case datapoint := <-forwarder.datapointsChannel:
			datapoints = append(datapoints, datapoint)
			glog.V(2).Infof("Got another point to increase size")
			continue
		default:
			glog.V(2).Infof("Nothing on channel: %d", len(forwarder.datapointsChannel))
			// Nothing left.  Flush this.
			break Loop
		}
	}
	return datapoints, nil
}

func (forwarder *basicBufferedForwarder) stop() {
	// Set stop flag
	atomic.StoreInt32(&forwarder.stopped, 1)
	// Send a stop signal
	myChan := make(chan bool, forwarder.numDrainingThreads)
	for i := 0; i < int(forwarder.numDrainingThreads); i++ {
		forwarder.blockingDrainStopChan <- myChan
	}

	// Wait for stop to come in
	for i := 0; i < int(forwarder.numDrainingThreads); i++ {
		_ = <-myChan
	}
}

func (forwarder *basicBufferedForwarder) start(processor ProcessingFunction) {
	if forwarder.started {
		glog.Fatalf("Forwarder already started!")
	}
	// TODO: Add stop?
	forwarder.started = true
	for i := uint32(0); i < forwarder.numDrainingThreads; i++ {
		go func() {
			for atomic.LoadInt32(&forwarder.stopped) == 0 {
				datapoints, err := forwarder.blockingDrainUpTo()
				if err != nil {
					glog.Warningf("Unable to drain any datapoints: %s", err)
					continue
				}
				err = processor(datapoints)
				if err != nil {
					glog.Warningf("Unable to process datapoints: %s", err)
					continue
				}
			}
		}()
	}
}

func (forwarder *basicBufferedForwarder) Name() string {
	return forwarder.name
}

// NewBasicBufferedForwarder is used only by this package to create a forwarder that buffers its
// datapoint channel
func NewBasicBufferedForwarder(bufferSize uint32, maxDrainSize uint32, name string, numDrainingThreads uint32) *basicBufferedForwarder {
	return &basicBufferedForwarder{
		datapointsChannel:  make(chan core.Datapoint, bufferSize),
		maxDrainSize:       maxDrainSize,
		bufferSize:         bufferSize,
		name:               name,
		numDrainingThreads: numDrainingThreads,
		started:            false,
	}
}

// AllForwarderLoaders is a map of config names to loaders for that config
var AllForwarderLoaders = map[string]ForwarderLoader{
	"signalfx-json": SignalfxJSONForwarderLoader,
	"carbon":        TcpGraphiteCarbonForwarerLoader,
	"csv":           CsvForwarderLoader,
}
