package ddagent

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/signalfx/golib/datapoint"
)

// Sent from http_emitter.py
type ddAgentIntakePayload struct {
	Metrics []ddAgentFormattedMetric `json:"metrics"`
}

// Defined in __init__.py:agent_formatter()
type ddAgentFormattedMetric struct {
	metric     string
	timestamp  int64
	value      json.Number
	attributes ddAttributes
}

var errUnsupportedMetricFormat = errors.New("metric format not supported")
var errUnsupportedValue = errors.New("value not of supported type")

func (d *ddAgentFormattedMetric) Datapoint() (*datapoint.Datapoint, error) {
	if d.metric == "" {
		return nil, errUnsupportedMetricFormat
	}
	var val datapoint.Value
	if numAsInt, err := d.value.Int64(); err == nil {
		val = datapoint.NewIntValue(numAsInt)
	} else if numAsFloat, err := d.value.Float64(); err == nil {
		val = datapoint.NewFloatValue(numAsFloat)
	} else {
		return nil, errUnsupportedValue
	}
	mtype, err := d.attributes.MetricType()
	if err != nil {
		return nil, err
	}
	dims := d.attributes.IntoDimensions()
	return datapoint.New(d.metric, dims, val, mtype, time.Unix(d.timestamp, 0)), nil
}

var errUnexpectedMetricLen = errors.New("unexpectec JSON[] len")

func (d *ddAgentFormattedMetric) UnmarshalJSON(b []byte) error {
	into := []json.RawMessage{}
	if err := json.Unmarshal(b, &into); err != nil {
		return err
	}
	if len(into) < 3 || len(into) > 4 {
		return errUnexpectedMetricLen
	}
	newMetric := ddAgentFormattedMetric{}
	if err := json.Unmarshal(into[0], &newMetric.metric); err != nil {
		return err
	}

	if err := json.Unmarshal(into[1], &newMetric.timestamp); err != nil {
		return err
	}

	if err := json.Unmarshal(into[2], &newMetric.value); err != nil {
		return err
	}

	if len(into) == 4 {
		if err := json.Unmarshal(into[3], &newMetric.attributes); err != nil {
			return err
		}
	}

	*d = newMetric
	return nil
}

var _ json.Unmarshaler = &ddAgentFormattedMetric{}

type ddAttributes struct {
	Tags       []string `json:"tags"`
	Hostname   *string  `json:"hostname"`
	DeviceName *string  `json:"device_name"`
	Type       *string  `json:"type"`
}

type unknownMetricType string

func (u unknownMetricType) Error() string {
	return fmt.Sprintf("unknown metric type: %s", string(u))
}

func (d *ddAttributes) MetricType() (datapoint.MetricType, error) {
	if d.Type == nil || *d.Type == "" {
		return datapoint.Gauge, nil
	}
	if *d.Type == "gauge" {
		return datapoint.Gauge, nil
	}
	return datapoint.Gauge, unknownMetricType(*d.Type)
}

func (d *ddAttributes) IntoDimensions() map[string]string {
	ret := make(map[string]string, len(d.Tags)+2)
	if d.Tags != nil {
		mergeMap(ret, dimensionsFromDDagentTags(d.Tags))
	}
	if d.Hostname != nil && *d.Hostname != "" {
		ret["host"] = *d.Hostname
	}
	if d.DeviceName != nil && *d.DeviceName != "" {
		ret["device"] = *d.DeviceName
	}
	return ret
}

func mergeMap(into map[string]string, from map[string]string) {
	for k, v := range from {
		into[k] = v
	}
}

func dimensionsFromDDagentTags(tags []string) map[string]string {
	r := make(map[string]string, len(tags))
	for _, tag := range tags {
		parts := strings.SplitN(tag, ":", 2)
		if len(parts) == 2 {
			r[parts[0]] = parts[1]
		}
		// Ambiguous what to do for tags that don't have a value ...
	}
	return r
}
