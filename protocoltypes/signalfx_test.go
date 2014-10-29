package protocoltypes

import (
	"github.com/cep21/gohelpers/a"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
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
	}
	a.ExpectContains(t, item.String(), "ametric", "Should get metric name back")
}
