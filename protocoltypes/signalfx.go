package protocoltypes

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
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

// ValueToDatapointValue converts the v2 JSON value to a core api Value
func ValueToDatapointValue(v ValueToSend) (value.DatapointValue, error) {
	f, ok := v.(float64)
	if ok {
		return value.NewFloatWire(f), nil
	}
	i, ok := v.(int64)
	if ok {
		return value.NewIntWire(i), nil
	}
	i2, ok := v.(int)
	if ok {
		return value.NewIntWire(int64(i2)), nil
	}
	s, ok := v.(string)
	if ok {
		return value.NewStrWire(s), nil
	}
	return nil, fmt.Errorf("Unable to convert value: %s", v)
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
	log.WithField("dp", datapoint).Debug("NewProtobufDataPointWithType")

	if datapoint.MetricType != nil {
		mt = datapoint.GetMetricType()
	} else {
		mt = mType
	}

	dims := map[string]string{}
	if datapoint.GetSource() != "" {
		dims["sf_source"] = datapoint.GetSource()
	}

	dpdims := datapoint.GetDimensions()
	for _, dpdim := range dpdims {
		dims[dpdim.GetKey()] = dpdim.GetValue()
	}

	return core.NewRelativeTimeDatapoint(datapoint.GetMetric(),
		dims, value.NewDatumWire(datapoint.GetValue()), mt,
		datapoint.GetTimestamp())
}
