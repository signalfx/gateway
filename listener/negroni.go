package listener

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
)

type DatapointTracker struct {
	TotalDatapoints       int64
	DatapointStreamingAPI core.DatapointStreamingAPI
}

func (t *DatapointTracker) AddDatapoint(dp core.Datapoint) {
	t.DatapointStreamingAPI.DatapointsChannel() <- dp
	atomic.AddInt64(&t.TotalDatapoints, 1)
}

func (t *DatapointTracker) GetStats(dimensions map[string]string) []core.Datapoint {
	ret := []core.Datapoint{}
	ret = append(
		ret,
		protocoltypes.NewOnHostDatapointDimensions(
			"total_datapoints",
			value.NewIntWire(t.TotalDatapoints),
			com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
			dimensions))
	return ret
}

type MetricTrackingMiddleware struct {
	TotalConnections      int64
	ActiveConnections     int64
	TotalProcessingTimeNs int64
}

func (m *MetricTrackingMiddleware) ServeHTTP(rw http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	atomic.AddInt64(&m.TotalConnections, 1)
	atomic.AddInt64(&m.ActiveConnections, 1)
	defer atomic.AddInt64(&m.ActiveConnections, 1)
	start := time.Now()
	next(rw, r)
	reqDuration := time.Since(start)
	atomic.AddInt64(&m.TotalProcessingTimeNs, reqDuration.Nanoseconds())
}

func (m *MetricTrackingMiddleware) GetStats(dimensions map[string]string) []core.Datapoint {
	ret := []core.Datapoint{}
	stats := map[string]int64{
		"total_connections": atomic.LoadInt64(&m.TotalConnections),
		"total_time_ns":     atomic.LoadInt64(&m.TotalProcessingTimeNs),
	}
	for k, v := range stats {
		ret = append(
			ret,
			protocoltypes.NewOnHostDatapointDimensions(
				k,
				value.NewIntWire(v),
				com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
				dimensions))
	}
	ret = append(
		ret,
		protocoltypes.NewOnHostDatapointDimensions(
			"active_connections",
			value.NewIntWire(atomic.LoadInt64(&m.ActiveConnections)),
			com_signalfuse_metrics_protobuf.MetricType_GAUGE,
			dimensions))
	return ret
}
