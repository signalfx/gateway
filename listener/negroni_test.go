package listener

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricTrackingMiddleware(t *testing.T) {
	m := MetricTrackingMiddleware{}
	f := func(rw http.ResponseWriter, r *http.Request) {
		assert.Equal(t, 1, m.ActiveConnections)
		assert.Equal(t, 1, m.TotalConnections)
		assert.Equal(t, 0, m.TotalProcessingTimeNs)
	}
	m.ServeHTTP(nil, nil, f)

	f = func(rw http.ResponseWriter, r *http.Request) {
		assert.Equal(t, 1, m.ActiveConnections)
		assert.Equal(t, 2, m.TotalConnections)
		assert.NotEqual(t, 0, m.TotalProcessingTimeNs)
	}
	m.ServeHTTP(nil, nil, f)
}
