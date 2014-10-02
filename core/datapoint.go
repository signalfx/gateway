package core

import (
	"fmt"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core/value"
	"time"
)

// A Datapoint is the metric that is saved
type Datapoint interface {
	// What is being measured
	Metric() string
	// Dimensions of what is being measured
	Dimensions() map[string]string
	// Value of the datapoint
	Value() value.DatapointValue
	// The type of the datapoint series
	MetricType() com_signalfuse_metrics_protobuf.MetricType
	// The unix time of the datapoint
	Timestamp() time.Time
	// String readable datapoint
	String() string
}

type baseDatapoint struct {
	metric     string
	dimensions map[string]string
	value      value.DatapointValue
	metricType com_signalfuse_metrics_protobuf.MetricType
}

func (datapoint *baseDatapoint) Metric() string {
	return datapoint.metric
}

func (datapoint *baseDatapoint) Dimensions() map[string]string {
	return datapoint.dimensions
}

func (datapoint *baseDatapoint) Value() value.DatapointValue {
	return datapoint.value
}

// The type of the datapoint series
func (datapoint *baseDatapoint) MetricType() com_signalfuse_metrics_protobuf.MetricType {
	return datapoint.metricType
}

func newBaseDatapoint(metric string, dimensions map[string]string, value value.DatapointValue, metricType com_signalfuse_metrics_protobuf.MetricType) *baseDatapoint {
	return &baseDatapoint{
		metric:     metric,
		dimensions: dimensions,
		value:      value,
		metricType: metricType,
	}
}

type absoluteTimeDatapoint struct {
	baseDatapoint
	timestamp time.Time
}

func (datapoint *absoluteTimeDatapoint) Timestamp() time.Time {
	return datapoint.timestamp
}

func (datapoint *absoluteTimeDatapoint) String() string {
	return fmt.Sprintf("%s\t%s\t%s\t%s\t%s", datapoint.Metric(), datapoint.Dimensions(), datapoint.Value(), datapoint.MetricType(), datapoint.Timestamp().String())
}

// NewAbsoluteTimeDatapoint creates a new datapoint who's time is an absolute value
func NewAbsoluteTimeDatapoint(metric string, dimensions map[string]string, value value.DatapointValue, metricType com_signalfuse_metrics_protobuf.MetricType, timestamp time.Time) Datapoint {
	return &absoluteTimeDatapoint{
		baseDatapoint: baseDatapoint{
			metric:     metric,
			dimensions: dimensions,
			value:      value,
			metricType: metricType,
		},
		timestamp: timestamp,
	}
}

type relativeTimeDatapoint struct {
	baseDatapoint
	relativeTime int64
}

func (datapoint *relativeTimeDatapoint) Timestamp() time.Time {
	if datapoint.relativeTime > 0 {
		return time.Unix(0, datapoint.relativeTime*int64(1000))
	}
	return time.Now().Add(time.Millisecond * time.Duration(datapoint.relativeTime))
}

func (datapoint *relativeTimeDatapoint) String() string {
	return fmt.Sprintf("%s\t%s\t%s\t%s\t%s", datapoint.Metric(), datapoint.Dimensions(), datapoint.Value().WireValue(), datapoint.MetricType(), datapoint.Timestamp().String())
}

// NewRelativeTimeDatapoint creates a new datapoint who's time is a value relative to when it's recieved
func NewRelativeTimeDatapoint(metric string, dimensions map[string]string, value value.DatapointValue, metricType com_signalfuse_metrics_protobuf.MetricType, relativeTime int64) Datapoint {
	return &relativeTimeDatapoint{
		baseDatapoint: baseDatapoint{
			metric:     metric,
			dimensions: dimensions,
			value:      value,
			metricType: metricType,
		},
		relativeTime: relativeTime,
	}
}
