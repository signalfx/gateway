package reqcounter

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/datapoint"
)

// RequestCounter is a negroni handler that tracks connection stats
type RequestCounter struct {
	TotalConnections      int64
	ActiveConnections     int64
	TotalProcessingTimeNs int64
}

func (m *RequestCounter) ServeHTTP(rw http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	atomic.AddInt64(&m.TotalConnections, 1)
	atomic.AddInt64(&m.ActiveConnections, 1)
	defer atomic.AddInt64(&m.ActiveConnections, -1)
	start := time.Now()
	next(rw, r)
	reqDuration := time.Since(start)
	atomic.AddInt64(&m.TotalProcessingTimeNs, reqDuration.Nanoseconds())
}

// Stats returns stats on total connections, active connections, and total processing time
func (m *RequestCounter) Stats(dimensions map[string]string) []datapoint.Datapoint {
	ret := []datapoint.Datapoint{}
	stats := map[string]int64{
		"total_connections": atomic.LoadInt64(&m.TotalConnections),
		"total_time_ns":     atomic.LoadInt64(&m.TotalProcessingTimeNs),
	}
	for k, v := range stats {
		ret = append(
			ret,
			datapoint.NewOnHostDatapointDimensions(
				k,
				datapoint.NewIntValue(v),
				com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
				dimensions))
	}
	ret = append(
		ret,
		datapoint.NewOnHostDatapointDimensions(
			"active_connections",
			datapoint.NewIntValue(atomic.LoadInt64(&m.ActiveConnections)),
			com_signalfuse_metrics_protobuf.MetricType_GAUGE,
			dimensions))
	return ret
}
