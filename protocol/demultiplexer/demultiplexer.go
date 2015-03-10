package demultiplexer

import (
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/stats"
)

type streamingDemultiplexerImpl struct {
	sendTo               []datapoint.NamedStreamer
	datapointChannel     chan datapoint.Datapoint
	droppedPoints        []int64
	totalDatapoints      int64
	name                 string
	latestDatapointDelay int64
}

func (streamer *streamingDemultiplexerImpl) Channel() chan<- datapoint.Datapoint {
	return streamer.datapointChannel
}

func (streamer *streamingDemultiplexerImpl) Name() string {
	return streamer.name
}

func (streamer *streamingDemultiplexerImpl) datapointReadingThread() {
	for {
		dp := <-streamer.datapointChannel
		// Don't do it all the time, could be slow
		if atomic.LoadInt64(&streamer.totalDatapoints)%1000 == 0 {
			delay := time.Now().Sub(dp.Timestamp())
			atomic.StoreInt64(&streamer.latestDatapointDelay, delay.Nanoseconds())
		}
		log.WithField("datapoint", dp).Debug("New datapoint")
		for index, sendTo := range streamer.sendTo {
			select {
			case sendTo.Channel() <- dp:
			default:
				// Don't block operation
				atomic.AddInt64(&streamer.droppedPoints[index], 1)
				log.Info("Dropped datapoint")
			}
		}
		atomic.AddInt64(&streamer.totalDatapoints, 1)
	}
}

func (streamer *streamingDemultiplexerImpl) Stats() []datapoint.Datapoint {
	ret := []datapoint.Datapoint{}
	for index := range streamer.droppedPoints {
		val := atomic.LoadInt64(&streamer.droppedPoints[index])
		ret = append(
			ret,
			datapoint.NewOnHostDatapointDimensions(
				"dropped_points",
				datapoint.NewIntValue(val),
				com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
				map[string]string{"forwarder": streamer.sendTo[index].Name()}))
	}
	totalDatapoints := atomic.LoadInt64(&streamer.totalDatapoints)
	ret = append(ret, datapoint.NewOnHostDatapointDimensions(
		"total_datapoints",
		datapoint.NewIntValue(totalDatapoints),
		com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		map[string]string{"forwarder": streamer.Name()}))
	ret = append(ret, datapoint.NewOnHostDatapointDimensions(
		"datapoint_backup_size",
		datapoint.NewIntValue(int64(len(streamer.datapointChannel))),
		com_signalfuse_metrics_protobuf.MetricType_GAUGE,
		map[string]string{"forwarder": streamer.Name()}))
	ret = append(ret, datapoint.NewOnHostDatapointDimensions(
		"datapoint_delay",
		datapoint.NewIntValue(atomic.LoadInt64(&streamer.latestDatapointDelay)),
		com_signalfuse_metrics_protobuf.MetricType_GAUGE,
		map[string]string{"forwarder": streamer.Name()}))
	return ret
}

// New creates a new forwarder that sends datapoints to multiple recievers
func New(sendTo []datapoint.NamedStreamer) stats.StatKeepingStreamer {
	ret := &streamingDemultiplexerImpl{
		sendTo,
		make(chan datapoint.Datapoint),
		make([]int64, len(sendTo)),
		0,
		"demultiplexer",
		0,
	}
	go ret.datapointReadingThread()
	return ret
}
