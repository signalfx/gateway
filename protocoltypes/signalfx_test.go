package protocoltypes

import (
	"github.com/cep21/gohelpers/a"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core/value"
	"testing"
)

func TestNewProtobufDataPoint(t *testing.T) {
	protoDatapoint := &com_signalfuse_metrics_protobuf.DataPoint{
		Source: workarounds.GolangDoesnotAllowPointerToStringLiteral("asource"),
		Metric: workarounds.GolangDoesnotAllowPointerToStringLiteral("ametric"),
		Value:  &com_signalfuse_metrics_protobuf.Datum{IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(2)},
	}
	dp := NewProtobufDataPointWithType(protoDatapoint, com_signalfuse_metrics_protobuf.MetricType_COUNTER)
	a.ExpectEquals(t, "asource", dp.Dimensions()["sf_source"], "Line should be invalid")
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_COUNTER, dp.MetricType(), "Line should be invalid")

	v := com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER
	protoDatapoint.MetricType = &v
	dp = NewProtobufDataPointWithType(protoDatapoint, com_signalfuse_metrics_protobuf.MetricType_COUNTER)
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, dp.MetricType(), "Line should be invalid")

	item := &BodySendFormatV2{
		Metric: "ametric",
		Value:  3.0,
	}
	a.ExpectContains(t, item.String(), "ametric", "Should get metric name back")
	f, _ := ValueToDatapointValue(item.Value)
	a.ExpectEquals(t, value.NewFloatWire(3.0), f, "Should get value 3 back")

	item.Value = 3
	i, _ := ValueToDatapointValue(item.Value)
	a.ExpectEquals(t, value.NewIntWire(3), i, "Should get value 3 back")

	item.Value = int64(3)
	ValueToDatapointValue(item.Value)

	item.Value = "abc"
	s, _ := ValueToDatapointValue(item.Value)
	a.ExpectEquals(t, value.NewStrWire("abc"), s, "Should get value abc back")

	item.Value = struct{}{}
	_, err := ValueToDatapointValue(item.Value)
	a.ExpectNotNil(t, err)
}
