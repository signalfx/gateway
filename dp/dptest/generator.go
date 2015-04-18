package dptest

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/signalfx/golib/datapoint"
)

// DatapointSource is a simple way to generate throw away datapoints
type DatapointSource struct {
	mu sync.Mutex

	CurrentIndex int64
	Metric       string
	Dims         map[string]string
	Dptype       datapoint.MetricType
	TimeSource   func() time.Time
}

var globalSource DatapointSource

func init() {
	globalSource.Metric = "random"
	globalSource.Dims = map[string]string{"source": "randtest"}
	globalSource.Dptype = datapoint.Gauge
	globalSource.TimeSource = time.Now
}

// Next returns a unique datapoint
func (d *DatapointSource) Next() *datapoint.Datapoint {
	d.mu.Lock()
	defer d.mu.Unlock()
	return datapoint.New(d.Metric+":"+strconv.FormatInt(atomic.AddInt64(&d.CurrentIndex, 1), 10), d.Dims, datapoint.NewIntValue(0), d.Dptype, d.TimeSource())
}

// DP generates and returns a unique datapoint to use for testing purposes
func DP() *datapoint.Datapoint {
	return globalSource.Next()
}
