package protocoltypes

import (
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"strings"
	"time"
)

// CollectdJSONWriteBody is the full POST body of collectd's write_http format
type CollectdJSONWriteBody []CollectdJSONWriteFormat

// CollectdJSONWriteFormat is the format for collectd json datapoints
type CollectdJSONWriteFormat struct {
	Dsnames        []*string  `json:"dsnames"`
	Dstypes        []*string  `json:"dstypes"`
	Host           *string    `json:"host"`
	Interval       *float64   `json:"interval"`
	Plugin         *string    `json:"plugin"`
	PluginInstance *string    `json:"plugin_instance"`
	Time           *float64   `json:"time"`
	TypeS          *string    `json:"type"`
	TypeInstance   *string    `json:"type_instance"`
	Values         []*float64 `json:"values"`
}

func metricTypeFromDsType(dstype *string) com_signalfuse_metrics_protobuf.MetricType {
	if dstype == nil {
		return com_signalfuse_metrics_protobuf.MetricType_GAUGE
	}

	m := map[string]com_signalfuse_metrics_protobuf.MetricType{
		"gauge":    com_signalfuse_metrics_protobuf.MetricType_GAUGE,
		"derive":   com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		"counter":  com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
		"absolute": com_signalfuse_metrics_protobuf.MetricType_COUNTER,
	}
	v, ok := m[*dstype]
	if ok {
		return v
	}
	return com_signalfuse_metrics_protobuf.MetricType_GAUGE
}

func isNilOrEmpty(str *string) bool {
	return str == nil || *str == ""
}

// NewCollectdDatapoint creates a new datapoint from collectd's write_http endpoint JSON format
func NewCollectdDatapoint(point CollectdJSONWriteFormat, index uint) core.Datapoint {
	dstype, val, dsname := point.Dstypes[index], point.Values[index], point.Dsnames[index]
	dimensions := make(map[string]string)
	metricType := metricTypeFromDsType(dstype)
	metricName := getReasonableMetricName(point, index)
	if point.Host != nil {
		dimensions["host"] = *point.Host
	}
	if point.Plugin != nil {
		dimensions["plugin"] = *point.Plugin
	}
	if point.PluginInstance != nil {
		dimensions["plugin_instance"] = *point.PluginInstance
	}
	if point.TypeInstance != nil {
		dimensions["type_instance"] = *point.TypeInstance
	}
	if point.TypeS != nil {
		dimensions["type"] = *point.TypeS
	}
	if dsname != nil {
		dimensions["dsname"] = *dsname
	}
	timestamp := time.Unix(0, int64(float64(time.Second)**point.Time))
	return core.NewAbsoluteTimeDatapoint(metricName, dimensions, value.NewFloatWire(*val), metricType, timestamp)
}

func getReasonableMetricName(point CollectdJSONWriteFormat, index uint) string {
	parts := []string{}
	if !isNilOrEmpty(point.TypeS) {
		parts = append(parts, *point.TypeS)
	}
	if !isNilOrEmpty(point.TypeInstance) {
		parts = append(parts, *point.TypeInstance)
	}
	if !isNilOrEmpty(point.Dsnames[index]) && len(point.Dsnames) > 1 {
		parts = append(parts, *point.Dsnames[index])
	}
	return strings.Join(parts, ".")
}
