package core

import (
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRelativeDatapoint(t *testing.T) {
	dp := NewRelativeTimeDatapoint("metric", map[string]string{"host": "bob"},
		value.NewIntWire(1), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, 0)
	assert.Equal(t, "metric", dp.Metric(), "Unexpected metric")
	assert.Equal(t, "bob", dp.Dimensions()["host"], "Unexpected dimensions")
	iv, err := dp.Value().IntValue()
	assert.Equal(t, int64(1), iv, "Unexpected value")
	assert.Equal(t, nil, err, "Unexpected value")
	assert.Equal(t, com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, dp.MetricType(), "Unexpected type")
	assert.Equal(t, com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, dp.MetricType(), "Unexpected time")

	now := time.Now()
	timeXXXXNow = func() time.Time { return now }
	defer func() { timeXXXXNow = time.Now }()

	assert.Equal(t, now, dp.Timestamp(), "Expected now")

	dp = NewRelativeTimeDatapoint("metric", map[string]string{"host": "bob"},
		value.NewIntWire(1), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, -1)
	assert.Equal(t, now.Add(-time.Millisecond), dp.Timestamp(), "Expected now one ms before")
	assert.Equal(t, int64(-1), dp.RelativeTime(), "Expected negative relative time")
	dp = NewRelativeTimeDatapoint("metric", map[string]string{"host": "bob"},
		value.NewIntWire(1), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, now.UnixNano()/int64(time.Millisecond))
	assert.Contains(t, dp.String(), "bob", "Got string for dp")
	assert.Equal(t, now.Round(time.Second), dp.Timestamp().Round(time.Second), "Expected exact unix time")
}

func TestAbsoluteDatapoint(t *testing.T) {
	curTime := time.Now()
	dp := NewAbsoluteTimeDatapoint("metric", map[string]string{"host": "bob"},
		value.NewIntWire(1), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, curTime)
	assert.Equal(t, curTime, dp.Timestamp(), "Expect absolute time")
	assert.Contains(t, dp.String(), "bob", "Got string for dp")
}
