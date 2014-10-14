package core

import (
	"github.com/cep21/gohelpers/a"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core/value"
	"testing"
	"time"
)

func TestRelativeDatapoint(t *testing.T) {
	dp := NewRelativeTimeDatapoint("metric", map[string]string{"host": "bob"},
		value.NewIntWire(1), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, 0)
	a.ExpectEquals(t, "metric", dp.Metric(), "Unexpected metric")
	a.ExpectEquals(t, "bob", dp.Dimensions()["host"], "Unexpected dimensions")
	iv, err := dp.Value().IntValue()
	a.ExpectEquals(t, int64(1), iv, "Unexpected value")
	a.ExpectEquals(t, nil, err, "Unexpected value")
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, dp.MetricType(), "Unexpected type")
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, dp.MetricType(), "Unexpected time")

	now := time.Now()
	timeXXXXNow = func() time.Time { return now }
	defer func() { timeXXXXNow = time.Now }()

	a.ExpectEquals(t, now, dp.Timestamp(), "Expected now")

	dp = NewRelativeTimeDatapoint("metric", map[string]string{"host": "bob"},
		value.NewIntWire(1), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, -1)
	a.ExpectEquals(t, now.Add(-time.Millisecond), dp.Timestamp(), "Expected now one ms before")
	a.ExpectEquals(t, int64(-1), dp.RelativeTime(), "Expected negative relative time")
	dp = NewRelativeTimeDatapoint("metric", map[string]string{"host": "bob"},
		value.NewIntWire(1), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, now.UnixNano()/int64(time.Millisecond))
	a.ExpectContains(t, dp.String(), "bob", "Got string for dp")
	a.ExpectEquals(t, now.Round(time.Second), dp.Timestamp().Round(time.Second), "Expected exact unix time")
}

func TestAbsoluteDatapoint(t *testing.T) {
	curTime := time.Now()
	dp := NewAbsoluteTimeDatapoint("metric", map[string]string{"host": "bob"},
		value.NewIntWire(1), com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, curTime)
	a.ExpectEquals(t, curTime, dp.Timestamp(), "Expect absolute time")
	a.ExpectContains(t, dp.String(), "bob", "Got string for dp")
}
