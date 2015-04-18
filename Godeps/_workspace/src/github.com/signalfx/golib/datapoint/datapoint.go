package datapoint

import (
	"fmt"
	"time"
)

// Documentation taken from http://metrics20.org/spec/

// MetricType define how to display the Value.  It's more metadata of the series than data about the
// series itself.  See target_type of http://metrics20.org/spec/
type MetricType int

const (
	// Gauge is values at each point in time
	Gauge MetricType = iota
	// Count is a number that keeps increasing over time
	Count
	// Enum is an added type: Values aren't important relative to each other but are just important as distinct
	//             items in a set.  Usually used when Value is type "string"
	Enum
	// Counter is a number per a given interval
	Counter
	// Rate is a number per second
	Rate
	// Timestamp value represents a unix timestamp
	Timestamp
)

// A Datapoint is the metric that is saved.  Designe around http://metrics20.org/spec/
type Datapoint struct {
	// What is being measured.  We think metric, rather than "unit" of metrics20, should be the
	// required identitiy of a datapoint and the "unit" should be a property of the Value itself
	Metric string
	// Dimensions of what is being measured.  They are intrinsic.  Contributes to the identity of
	// the metric. If this changes, we get a new metric identifier
	Dimensions map[string]string
	// Meta is information that's not paticularly important to the datapoint, but may be important
	// to the pipeline that uses the datapoint.  They are extrinsic.  It provides additional
	// information about the metric. changes in this set doesn't change the metric identity
	Meta map[interface{}]interface{}
	// Value of the datapoint
	Value Value
	// The type of the datapoint series
	MetricType MetricType
	// The unix time of the datapoint
	Timestamp time.Time
}

func (dp *Datapoint) String() string {
	return fmt.Sprintf("DP[%s\t%s\t%s\t%d\t%s]", dp.Metric, dp.Dimensions, dp.Value, dp.MetricType, dp.Timestamp.String())
}

// New creates a new datapoint with empty meta data
func New(metric string, dimensions map[string]string, value Value, metricType MetricType, timestamp time.Time) *Datapoint {
	return NewWithMeta(metric, dimensions, map[interface{}]interface{}{}, value, metricType, timestamp)
}

// NewWithMeta creates a new datapoint with passed metadata
func NewWithMeta(metric string, dimensions map[string]string, meta map[interface{}]interface{}, value Value, metricType MetricType, timestamp time.Time) *Datapoint {
	return &Datapoint{
		Metric:     metric,
		Dimensions: dimensions,
		Meta:       meta,
		Value:      value,
		MetricType: metricType,
		Timestamp:  timestamp,
	}
}
