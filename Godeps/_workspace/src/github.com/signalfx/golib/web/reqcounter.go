package web

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/signalfx/golib/datapoint"
)

// RequestCounter is a negroni handler that tracks connection stats
type RequestCounter struct {
	TotalConnections      int64
	ActiveConnections     int64
	TotalProcessingTimeNs int64
}

var _ HTTPConstructor = (&RequestCounter{}).Wrap
var _ NextHTTP = (&RequestCounter{}).ServeHTTP

// Wrap returns a handler that forwards calls to next and counts the calls forwarded
func (m *RequestCounter) Wrap(next http.Handler) http.Handler {
	f := func(w http.ResponseWriter, r *http.Request) {
		m.ServeHTTP(w, r, next)
	}
	return http.HandlerFunc(f)
}

func (m *RequestCounter) ServeHTTP(rw http.ResponseWriter, r *http.Request, next http.Handler) {
	atomic.AddInt64(&m.TotalConnections, 1)
	atomic.AddInt64(&m.ActiveConnections, 1)
	defer atomic.AddInt64(&m.ActiveConnections, -1)
	start := time.Now()
	next.ServeHTTP(rw, r)
	reqDuration := time.Since(start)
	atomic.AddInt64(&m.TotalProcessingTimeNs, reqDuration.Nanoseconds())
}

// Stats returns stats on total connections, active connections, and total processing time
func (m *RequestCounter) Stats(dimensions map[string]string) []*datapoint.Datapoint {
	ret := []*datapoint.Datapoint{}
	stats := map[string]int64{
		"total_connections": atomic.LoadInt64(&m.TotalConnections),
		"total_time_ns":     atomic.LoadInt64(&m.TotalProcessingTimeNs),
	}
	for k, v := range stats {
		ret = append(
			ret,
			datapoint.New(
				k,
				dimensions,
				datapoint.NewIntValue(v),
				datapoint.Counter,
				time.Now()))
	}
	ret = append(
		ret,
		datapoint.New(
			"active_connections",
			dimensions,
			datapoint.NewIntValue(atomic.LoadInt64(&m.ActiveConnections)),
			datapoint.Gauge,
			time.Now()))
	return ret
}
