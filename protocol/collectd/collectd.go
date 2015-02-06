package collectd

import (
	"strings"
	"time"

	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/datapoint"
)

// JSONWriteBody is the full POST body of collectd's write_http format
type JSONWriteBody []*JSONWriteFormat

// JSONWriteFormat is the format for collectd json datapoints
type JSONWriteFormat struct {
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
func NewCollectdDatapoint(point *JSONWriteFormat, index uint) datapoint.Datapoint {
	dstype, val, dsname := point.Dstypes[index], point.Values[index], point.Dsnames[index]
	dimensions := make(map[string]string)
	metricType := metricTypeFromDsType(dstype)
	metricName, usedParts := getReasonableMetricName(point, index)
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
		_, usedInMetricName := usedParts["type_instance"]
		if !usedInMetricName {
			dimensions["type_instance"] = *point.TypeInstance
		}
	}
	if point.TypeS != nil {
		_, usedInMetricName := usedParts["type"]
		if !usedInMetricName {
			dimensions["type"] = *point.TypeS
		}
	}
	if dsname != nil {
		_, usedInMetricName := usedParts["dsname"]
		if !usedInMetricName {
			dimensions["dsname"] = *dsname
		}
	}
	timestamp := time.Unix(0, int64(float64(time.Second)**point.Time))
	return datapoint.NewAbsoluteTime(metricName, dimensions, datapoint.NewFloatValue(*val), metricType, timestamp)
}

func getReasonableMetricName(point *JSONWriteFormat, index uint) (string, map[string]struct{}) {
	parts := []string{}
	usedParts := make(map[string]struct{})
	if !isNilOrEmpty(point.TypeS) {
		parts = append(parts, *point.TypeS)
		usedParts["type"] = struct{}{}
	}
	if !isNilOrEmpty(point.TypeInstance) {
		parts = append(parts, *point.TypeInstance)
		usedParts["type_instance"] = struct{}{}
	}
	if !isNilOrEmpty(point.Dsnames[index]) && len(point.Dsnames) > 1 {
		parts = append(parts, *point.Dsnames[index])
		usedParts["dsname"] = struct{}{}
	}
	return strings.Join(parts, "."), usedParts
}
