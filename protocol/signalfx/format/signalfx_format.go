package signalfxformat

import "fmt"

// JSONDatapointV1 is the JSON API format for /v1/datapoint
//easyjson:json
type JSONDatapointV1 struct {
	//easyjson:json
	Source string  `json:"source"`
	Metric string  `json:"metric"`
	Value  float64 `json:"value"`
}

// JSONDatapointV2 is the V2 json datapoint sending format
//easyjson:json
type JSONDatapointV2 map[string][]*BodySendFormatV2

// BodySendFormatV2 is the JSON format signalfx datapoints are expected to be in
//easyjson:json
type BodySendFormatV2 struct {
	Metric     string                 `json:"metric"`
	Timestamp  int64                  `json:"timestamp"`
	Value      ValueToSend            `json:"value"`
	Dimensions map[string]string      `json:"dimensions"`
	Properties map[string]ValueToSend `json:"properties"`
}

func (bodySendFormat *BodySendFormatV2) String() string {
	return fmt.Sprintf("DP[metric=%s|time=%d|val=%s|dimensions=%s|props=%s]", bodySendFormat.Metric, bodySendFormat.Timestamp, bodySendFormat.Value, bodySendFormat.Dimensions, bodySendFormat.Properties)
}

// ValueToSend are values are sent from the proxy to a receiver for the datapoint
type ValueToSend interface{}

// JSONEventV2 is the V2 json event sending format
//easyjson:json
type JSONEventV2 []*EventSendFormatV2

// EventSendFormatV2 is the JSON format signalfx datapoints are expected to be in
//easyjson:json
type EventSendFormatV2 struct {
	EventType  string                 `json:"eventType"`
	Category   *string                `json:"category"`
	Dimensions map[string]string      `json:"dimensions"`
	Properties map[string]interface{} `json:"properties"`
	Timestamp  *int64                 `json:"timestamp"`
}
