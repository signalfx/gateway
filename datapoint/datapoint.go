package datapoint

import (
	"fmt"
	"time"

	"sync/atomic"

	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
)

// A Datapoint is the metric that is saved
type Datapoint interface {
	// What is being measured
	Metric() string
	// Dimensions of what is being measured
	Dimensions() map[string]string
	// Value of the datapoint
	Value() Value
	// The type of the datapoint series
	MetricType() com_signalfuse_metrics_protobuf.MetricType
	// The unix time of the datapoint
	Timestamp() time.Time
	// String readable datapoint (@deprecated)
	String() string
}

// Streamer is the interface servers we send data to
// must implement
type Streamer interface {
	Channel() chan<- Datapoint
}

// A NamedStreamer can stream datapoints and identify itself.  Useful for logging errors or metrics
// if you are using a non behaving streamer.
type NamedStreamer interface {
	Streamer
	Name() string
}

// TimeRelativeDatapoint is a datapoint that supports the optional ability to get the timestamp
// as a time relative to the current time, in Ms
type TimeRelativeDatapoint interface {
	Datapoint
	RelativeTime() int64
}

type baseDatapoint struct {
	metric     string
	dimensions map[string]string
	value      Value
	metricType com_signalfuse_metrics_protobuf.MetricType
}

func (dp *baseDatapoint) Metric() string {
	return dp.metric
}

func (dp *baseDatapoint) Dimensions() map[string]string {
	return dp.dimensions
}

func (dp *baseDatapoint) Value() Value {
	return dp.value
}

// The type of the datapoint series
func (dp *baseDatapoint) MetricType() com_signalfuse_metrics_protobuf.MetricType {
	return dp.metricType
}

type absoluteTimeDatapoint struct {
	baseDatapoint
	timestamp time.Time
}

func (dp *absoluteTimeDatapoint) Timestamp() time.Time {
	return dp.timestamp
}

func (dp *absoluteTimeDatapoint) String() string {
	return fmt.Sprintf("AbsDP[%s\t%s\t%s\t%s\t%s]", dp.Metric(), dp.Dimensions(), dp.Value(), dp.MetricType(), dp.Timestamp().String())
}

// NewAbsoluteTime creates a new datapoint who's time is an absolute value
func NewAbsoluteTime(metric string, dimensions map[string]string, value Value, metricType com_signalfuse_metrics_protobuf.MetricType, timestamp time.Time) Datapoint {
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

var timeXXXXNow = time.Now

func (dp *relativeTimeDatapoint) Timestamp() time.Time {
	if dp.relativeTime > 0 {
		return time.Unix(0, dp.relativeTime*int64(1000*1000))
	}
	return timeXXXXNow().Add(time.Millisecond * time.Duration(dp.relativeTime))
}

func (dp *relativeTimeDatapoint) RelativeTime() int64 {
	return dp.relativeTime
}

func (dp *relativeTimeDatapoint) String() string {
	return fmt.Sprintf("RelDP[%s\t%s\t%s\t%s\t%s(%d)]", dp.Metric(), dp.Dimensions(), dp.Value(), dp.MetricType(), dp.Timestamp().String(), dp.relativeTime)
}

// NewRelativeTime creates a new datapoint who's time is a value relative to when it's recieved
func NewRelativeTime(metric string, dimensions map[string]string, value Value, metricType com_signalfuse_metrics_protobuf.MetricType, relativeTime int64) TimeRelativeDatapoint {
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

// Tracker counts datapoints given to a streaming API
type Tracker struct {
	TotalDatapoints int64
	Streamer        Streamer
}

// Adder can receive datapoints.  Usually these are just forwarded to a channel.
type Adder interface {
	AddDatapoint(dp Datapoint)
}

// AddrFunc is a wrapper for any function that takes a datapoint, to turn it into an Addr type
type AddrFunc func(Datapoint)

// AddDatapoint to this caller, forwarding it to the wrapped function
func (a AddrFunc) AddDatapoint(dp Datapoint) {
	a(dp)
}

// AddDatapoint to a tracking, sending it to the channel
func (t *Tracker) AddDatapoint(dp Datapoint) {
	t.Streamer.Channel() <- dp
	atomic.AddInt64(&t.TotalDatapoints, 1)
}

// Stats returns the number of calls to AddDatapoint
func (t *Tracker) Stats(dimensions map[string]string) []Datapoint {
	ret := []Datapoint{}
	ret = append(
		ret,
		NewOnHostDatapointDimensions(
			"total_datapoints",
			NewIntValue(t.TotalDatapoints),
			com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
			dimensions))
	return ret
}
