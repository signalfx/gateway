package signalfx

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/datapoint"
)

// JSONDatapointV1 is the JSON API format for /v1/datapoint
type JSONDatapointV1 struct {
	Source string  `json:"source"`
	Metric string  `json:"metric"`
	Value  float64 `json:"value"`
}

// ValueToSend are values are sent from the proxy to a reciever for the datapoint
type ValueToSend interface {
}

// ValueToValue converts the v2 JSON value to a core api Value
func ValueToValue(v ValueToSend) (datapoint.Value, error) {
	f, ok := v.(float64)
	if ok {
		return datapoint.NewFloatValue(f), nil
	}
	i, ok := v.(int64)
	if ok {
		return datapoint.NewIntValue(i), nil
	}
	i2, ok := v.(int)
	if ok {
		return datapoint.NewIntValue(int64(i2)), nil
	}
	s, ok := v.(string)
	if ok {
		return datapoint.NewStringValue(s), nil
	}
	return nil, fmt.Errorf("Unable to convert value: %s", v)
}

// JSONDatapointV2 is the V2 json datapoint sending format
type JSONDatapointV2 map[string][]*BodySendFormatV2

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

// MetricCreationStruct is the API format for /v1/metric POST
type MetricCreationStruct struct {
	MetricName string `json:"sf_metric"`
	MetricType string `json:"sf_metricType"`
}

// MetricCreationResponse is the API response for /v1/metric POST
type MetricCreationResponse struct {
	Code    int    `json:"code,omitempty"`
	Error   bool   `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
}

// NewProtobufDataPointWithType creates a new datapoint from SignalFx's protobuf definition (backwards compatable with old API)
func NewProtobufDataPointWithType(dp *com_signalfuse_metrics_protobuf.DataPoint, mType com_signalfuse_metrics_protobuf.MetricType) datapoint.Datapoint {
	var mt com_signalfuse_metrics_protobuf.MetricType
	log.WithField("dp", dp).Debug("NewProtobufDataPointWithType")

	if dp.MetricType != nil {
		mt = dp.GetMetricType()
	} else {
		mt = mType
	}

	dims := map[string]string{}
	if dp.GetSource() != "" {
		dims["sf_source"] = dp.GetSource()
	}

	dpdims := dp.GetDimensions()
	for _, dpdim := range dpdims {
		dims[dpdim.GetKey()] = dpdim.GetValue()
	}

	return datapoint.NewRelativeTime(dp.GetMetric(),
		dims, datapoint.NewDatumValue(dp.GetValue()), mt,
		dp.GetTimestamp())
}
