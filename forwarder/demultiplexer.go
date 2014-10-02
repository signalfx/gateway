package forwarder

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
	"sync/atomic"
)

type streamingDemultiplexerImpl struct {
	sendTo           []core.DatapointStreamingAPI
	datapointChannel chan core.Datapoint
	droppedPoints    []int64
	totalDatapoints  *int64
	name             string
}

func (streamer *streamingDemultiplexerImpl) DatapointsChannel() chan<- core.Datapoint {
	return streamer.datapointChannel
}

func (streamer *streamingDemultiplexerImpl) Name() string {
	return streamer.name
}

func (streamer *streamingDemultiplexerImpl) datapointReadingThread() {
	for {
		datapoint := <-streamer.datapointChannel
		atomic.AddInt64(streamer.totalDatapoints, 1)
		glog.V(2).Infof("New datapoint: %s", datapoint)
		for index, sendTo := range streamer.sendTo {
			select {
			case sendTo.DatapointsChannel() <- datapoint:
			default:
				atomic.AddInt64(&streamer.droppedPoints[index], 1)
				glog.Info("Dropped datapoint")
				// Don't block operation
			}
		}
	}
}

func (streamer *streamingDemultiplexerImpl) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
	for index := range streamer.droppedPoints {
		val := atomic.LoadInt64(&streamer.droppedPoints[index])
		ret = append(ret, protocoltypes.NewOnHostDatapoint(fmt.Sprintf("proxy.droppedPoints.%s", streamer.sendTo[index].Name()), value.NewIntWire(val), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER))
	}
	totalDatapoints := atomic.LoadInt64(streamer.totalDatapoints)
	ret = append(ret, protocoltypes.NewOnHostDatapoint("proxy.total_datapoints", value.NewIntWire(totalDatapoints), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER))
	return ret
}

// NewStreamingDatapointDemultiplexer creates a new forwarder that sends datapoints to multiple recievers
func NewStreamingDatapointDemultiplexer(sendTo []core.DatapointStreamingAPI) (core.StatKeepingStreamingAPI, error) {
	ret := &streamingDemultiplexerImpl{
		sendTo,
		make(chan core.Datapoint),
		make([]int64, len(sendTo)),
		new(int64),
		"demultiplexer",
	}
	go ret.datapointReadingThread()
	return ret, nil
}
