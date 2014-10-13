package protocoltypes

import (
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
)

// SignalfxJSONDatapointV1 is the JSON API format for /v1/datapoint
type SignalfxJSONDatapointV1 struct {
	Source string  `json:"source"`
	Metric string  `json:"metric"`
	Value  float64 `json:"value"`
}

// SignalfxMetricCreationStruct is the API format for /v1/metric POST
type SignalfxMetricCreationStruct struct {
	MetricName string `json:"sf_metric"`
	MetricType string `json:"sf_metricType"`
}

// SignalfxMetricCreationResponse is the API response for /v1/metric POST
type SignalfxMetricCreationResponse struct {
	Code    int    `json:"code,omitempty"`
	Error   bool   `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
}

// NewProtobufDataPoint creates a new datapoint from SignalFx's protobuf definition
func NewProtobufDataPoint(datapoint com_signalfuse_metrics_protobuf.DataPoint) core.Datapoint {
	return core.NewRelativeTimeDatapoint(datapoint.GetMetric(),
		map[string]string{"sf_source": datapoint.GetSource()},
		value.NewDatumWire(datapoint.GetValue()), datapoint.GetMetricType(),
		datapoint.GetTimestamp())
}

// NewProtobufDataPointWithType creates a new datapoint from SignalFx's protobuf definition (backwards compatable with old API)
func NewProtobufDataPointWithType(datapoint com_signalfuse_metrics_protobuf.DataPoint, mType com_signalfuse_metrics_protobuf.MetricType) core.Datapoint {
	var mt com_signalfuse_metrics_protobuf.MetricType

	if datapoint.MetricType != nil {
		mt = datapoint.GetMetricType()
	} else {
		mt = mType
	}

	return core.NewRelativeTimeDatapoint(datapoint.GetMetric(),
		map[string]string{"sf_source": datapoint.GetSource()},
		value.NewDatumWire(datapoint.GetValue()), mt,
		datapoint.GetTimestamp())
}
