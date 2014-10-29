package protocoltypes

import (
	"fmt"
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

// ValueToSend are values are sent from the proxy to a reciever for the datapoint
type ValueToSend interface {
}

// SignalfxJSONDatapointV2 is the V2 json datapoint sending format
type SignalfxJSONDatapointV2 map[string][]*BodySendFormatV2

// BodySendFormatV2 is the JSON format signalfx datapoints are expected to be in
type BodySendFormatV2 struct {
	Metric     string            `json:"metric"`
	Timestamp  int64             `json:"timestamp"`
	Value      ValueToSend       `json:"value"`
	Dimensions map[string]string `json:"dimensions"`
}

func (bodySendFormat *BodySendFormatV2) String() string {
	return fmt.Sprintf("DP[metric=%s|time=%d|val=%s|dimensions=%s]", bodySendFormat.Metric, bodySendFormat.Timestamp, bodySendFormat.Value, bodySendFormat.Dimensions)
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

// NewProtobufDataPointWithType creates a new datapoint from SignalFx's protobuf definition (backwards compatable with old API)
func NewProtobufDataPointWithType(datapoint *com_signalfuse_metrics_protobuf.DataPoint, mType com_signalfuse_metrics_protobuf.MetricType) core.Datapoint {
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
