package datapoint

import (
	"testing"
	"time"

	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/stretchr/testify/assert"
)

func TestRelativeDatapoint(t *testing.T) {
	dp := NewRelativeTime("metric", map[string]string{"host": "bob"},
		NewIntValue(1), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, 0)
	assert.Equal(t, "metric", dp.Metric(), "Unexpected metric")
	assert.Equal(t, "bob", dp.Dimensions()["host"], "Unexpected dimensions")
	iv := dp.Value().(IntValue).Int()
	assert.Equal(t, int64(1), iv, "Unexpected value")
	assert.Equal(t, com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, dp.MetricType(), "Unexpected type")
	assert.Equal(t, com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, dp.MetricType(), "Unexpected time")

	now := time.Now()
	timeXXXXNow = func() time.Time { return now }
	defer func() { timeXXXXNow = time.Now }()

	assert.Equal(t, now, dp.Timestamp(), "Expected now")

	dp = NewRelativeTime("metric", map[string]string{"host": "bob"},
		NewIntValue(1), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, -1)
	assert.Equal(t, now.Add(-time.Millisecond), dp.Timestamp(), "Expected now one ms before")
	assert.Equal(t, int64(-1), dp.RelativeTime(), "Expected negative relative time")
	dp = NewRelativeTime("metric", map[string]string{"host": "bob"},
		NewIntValue(1), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, now.UnixNano()/int64(time.Millisecond))
	assert.Contains(t, dp.String(), "bob", "Got string for dp")
	assert.Equal(t, now.Round(time.Second), dp.Timestamp().Round(time.Second), "Expected exact unix time")
}

func TestAbsoluteDatapoint(t *testing.T) {
	curTime := time.Now()
	dp := NewAbsoluteTime("metric", map[string]string{"host": "bob"},
		NewIntValue(1), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, curTime)
	assert.Equal(t, curTime, dp.Timestamp(), "Expect absolute time")
	assert.Contains(t, dp.String(), "bob", "Got string for dp")
}

func TestTracker(t *testing.T) {
	s := NewBufferedForwarder(10, 1, "", 1)
	r := Tracker{
		Streamer: s,
	}
	r.AddDatapoint(nil)
	assert.Equal(t, 1, len(r.Stats(map[string]string{})))
}
