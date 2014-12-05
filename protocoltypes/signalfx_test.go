package protocoltypes

import (
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewProtobufDataPoint(t *testing.T) {
	protoDatapoint := &com_signalfuse_metrics_protobuf.DataPoint{
		Source: workarounds.GolangDoesnotAllowPointerToStringLiteral("asource"),
		Metric: workarounds.GolangDoesnotAllowPointerToStringLiteral("ametric"),
		Value:  &com_signalfuse_metrics_protobuf.Datum{IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(2)},
	}
	dp := NewProtobufDataPointWithType(protoDatapoint, com_signalfuse_metrics_protobuf.MetricType_COUNTER)
	assert.Equal(t, "asource", dp.Dimensions()["sf_source"], "Line should be invalid")
	assert.Equal(t, com_signalfuse_metrics_protobuf.MetricType_COUNTER, dp.MetricType(), "Line should be invalid")

	v := com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER
	protoDatapoint.MetricType = &v
	dp = NewProtobufDataPointWithType(protoDatapoint, com_signalfuse_metrics_protobuf.MetricType_COUNTER)
	assert.Equal(t, com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, dp.MetricType(), "Line should be invalid")

	item := &BodySendFormatV2{
		Metric: "ametric",
		Value:  3.0,
	}
	assert.Contains(t, item.String(), "ametric", "Should get metric name back")
	f, _ := ValueToDatapointValue(item.Value)
	assert.Equal(t, value.NewFloatWire(3.0), f, "Should get value 3 back")

	item.Value = 3
	i, _ := ValueToDatapointValue(item.Value)
	assert.Equal(t, value.NewIntWire(3), i, "Should get value 3 back")

	item.Value = int64(3)
	ValueToDatapointValue(item.Value)

	item.Value = "abc"
	s, _ := ValueToDatapointValue(item.Value)
	assert.Equal(t, value.NewStrWire("abc"), s, "Should get value abc back")

	item.Value = struct{}{}
	_, err := ValueToDatapointValue(item.Value)
	assert.Error(t, err)
}
