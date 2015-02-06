package signalfx

import (
	"testing"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"

	"github.com/signalfuse/signalfxproxy/datapoint"
	"github.com/stretchr/testify/assert"
)

func TestNewProtobufDataPoint(t *testing.T) {
	protoDatapoint := &com_signalfuse_metrics_protobuf.DataPoint{
		Source: workarounds.GolangDoesnotAllowPointerToStringLiteral("asource"),
		Metric: workarounds.GolangDoesnotAllowPointerToStringLiteral("ametric"),
		Value:  &com_signalfuse_metrics_protobuf.Datum{IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(2)},
		Dimensions: []*com_signalfuse_metrics_protobuf.Dimension{{
			Key:   workarounds.GolangDoesnotAllowPointerToStringLiteral("key"),
			Value: workarounds.GolangDoesnotAllowPointerToStringLiteral("value"),
		}},
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
	f, _ := ValueToValue(item.Value)
	assert.Equal(t, datapoint.NewFloatValue(3.0), f, "Should get value 3 back")

	item.Value = 3
	i, _ := ValueToValue(item.Value)
	assert.Equal(t, datapoint.NewIntValue(3), i, "Should get value 3 back")

	item.Value = int64(3)
	ValueToValue(item.Value)

	item.Value = "abc"
	s, _ := ValueToValue(item.Value)
	assert.Equal(t, datapoint.NewStringValue("abc"), s, "Should get value abc back")

	item.Value = struct{}{}
	_, err := ValueToValue(item.Value)
	assert.Error(t, err)
}
