package protocoltypes

import (
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
)

// NewProtobufDataPoint creates a new datapoint from SignalFx's protobuf definition
func NewProtobufDataPoint(datapoint com_signalfuse_metrics_protobuf.DataPoint) core.Datapoint {
	return core.NewRelativeTimeDatapoint(datapoint.GetMetric(),
		map[string]string{"sf_source": datapoint.GetSource()},
		value.NewDatumWire(datapoint.GetValue()), datapoint.GetMetricType(),
		datapoint.GetTimestamp())
}
