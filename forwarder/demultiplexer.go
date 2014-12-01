package forwarder

import (
	"github.com/golang/glog"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
	"sync/atomic"
	"time"
)

type streamingDemultiplexerImpl struct {
	sendTo               []core.DatapointStreamingAPI
	datapointChannel     chan core.Datapoint
	droppedPoints        []int64
	totalDatapoints      int64
	name                 string
	latestDatapointDelay int64
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
		// Don't do it all the time, could be slow
		if atomic.LoadInt64(&streamer.totalDatapoints)%1000 == 0 {
			delay := time.Now().Sub(datapoint.Timestamp())
			atomic.StoreInt64(&streamer.latestDatapointDelay, delay.Nanoseconds())
		}
		atomic.AddInt64(&streamer.totalDatapoints, 1)
		glog.V(2).Infof("New datapoint: %s", datapoint)
		for index, sendTo := range streamer.sendTo {
			select {
			case sendTo.DatapointsChannel() <- datapoint:
			default:
				// Don't block operation
				atomic.AddInt64(&streamer.droppedPoints[index], 1)
				glog.Infof("Dropped datapoint for %s", sendTo.Name())
			}
		}
	}
}

func (streamer *streamingDemultiplexerImpl) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
	for index := range streamer.droppedPoints {
		val := atomic.LoadInt64(&streamer.droppedPoints[index])
		ret = append(
			ret,
			protocoltypes.NewOnHostDatapointDimensions(
				"dropped_points",
				value.NewIntWire(val),
				com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
				map[string]string{"forwarder": streamer.sendTo[index].Name()}))
	}
	totalDatapoints := atomic.LoadInt64(&streamer.totalDatapoints)
	ret = append(ret, protocoltypes.NewOnHostDatapointDimensions(
		"total_datapoints",
		value.NewIntWire(totalDatapoints),
		com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		map[string]string{"forwarder": streamer.Name()}))
	ret = append(ret, protocoltypes.NewOnHostDatapointDimensions(
		"datapoint_delay",
		value.NewIntWire(atomic.LoadInt64(&streamer.latestDatapointDelay)),
		com_signalfuse_metrics_protobuf.MetricType_GAUGE,
		map[string]string{"forwarder": streamer.Name()}))
	return ret
}

// NewStreamingDatapointDemultiplexer creates a new forwarder that sends datapoints to multiple recievers
func NewStreamingDatapointDemultiplexer(sendTo []core.DatapointStreamingAPI) core.StatKeepingStreamingAPI {
	ret := &streamingDemultiplexerImpl{
		sendTo,
		make(chan core.Datapoint),
		make([]int64, len(sendTo)),
		0,
		"demultiplexer",
		0,
	}
	go ret.datapointReadingThread()
	return ret
}
