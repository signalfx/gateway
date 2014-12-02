package forwarder

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"sync"
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
	threadsWaitingToDie   sync.WaitGroup
	blockingDrainStopChan chan bool
}

// Loader is the function definition of a function that can load a config
// for a proxy and return the streamer
type Loader func(*config.ForwardTo) (core.StatKeepingStreamingAPI, error)

func (forwarder *basicBufferedForwarder) DatapointsChannel() chan<- core.Datapoint {
	return forwarder.datapointsChannel
}

func (forwarder *basicBufferedForwarder) blockingDrainUpTo() []core.Datapoint {
	// Block for at least one point
	datapoints := []core.Datapoint{}

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
		log.WithFields(log.Fields{"len": len(datapoints), "maxDrain": forwarder.maxDrainSize, "chanSize": len(forwarder.datapointsChannel)}).Debug("Less than size")
		select {
		case datapoint := <-forwarder.datapointsChannel:
			datapoints = append(datapoints, datapoint)
			log.Debug("Got another point to increase size")
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
				err := processor(datapoints)
				if err != nil {
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
