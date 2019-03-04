package dptest

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/trace"
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
	globalEventSource.EventType = "imanotify.notify_instance"
	globalEventSource.Dims = map[string]string{"host": "mwp-signalbox", "plugin": "my_plugin", "f": "x", "plugin_instance": "my_plugin_instance", "k": "v"}
	globalEventSource.Meta = map[string]interface{}{"string": "value", "boolean": true, "int": int64(40), "double": 0.0}
	globalEventSource.TimeSource = time.Now
	globalEventSource.Category = event.COLLECTD
	globalSpanSource.TimeSource = time.Now().UnixNano
	globalSpanSource.Name = "test-span"
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

// EventSource is a simple way to generate throw away events
type EventSource struct {
	mu sync.Mutex

	CurrentIndex int64
	EventType    string
	Category     event.Category
	Dims         map[string]string
	Meta         map[string]interface{}
	TimeSource   func() time.Time
}

var globalEventSource EventSource

// Next returns a unique event
func (d *EventSource) Next() *event.Event {
	d.mu.Lock()
	defer d.mu.Unlock()

	dims := make(map[string]string, len(d.Dims)+1)
	for k, v := range d.Dims {
		dims[k] = v
	}
	dims["index"] = strconv.FormatInt(atomic.AddInt64(&d.CurrentIndex, 1), 10)

	return event.NewWithProperties(d.EventType, d.Category, d.Dims, d.Meta, d.TimeSource())
}

// E generates and returns a unique event to use for testing purposes
func E() *event.Event {
	return globalEventSource.Next()
}

// SpanSource is a simple way to generate throw away spans
type SpanSource struct {
	mu sync.Mutex

	TimeSource   func() int64
	Name         string
	CurrentIndex int64
}

var globalSpanSource SpanSource

// Next return a unique span
func (d *SpanSource) Next() *trace.Span {
	d.mu.Lock()
	defer d.mu.Unlock()

	randomID := strconv.FormatInt(d.TimeSource(), 16)
	return &trace.Span{
		TraceID:   randomID,
		ID:        randomID,
		Name:      &d.Name,
		Timestamp: pointer.Int64(atomic.AddInt64(&d.CurrentIndex, 1)),
		Duration:  pointer.Int64((d.CurrentIndex % 15) + 5), // set duration in the range [5..20}
	}
}

// S generates and returns a unique span to use for testing purposes
func S() *trace.Span {
	return globalSpanSource.Next()
}
