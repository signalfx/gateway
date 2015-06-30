package collectd

import (
	"strings"
	"time"

	"github.com/signalfx/golib/datapoint"
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

func metricTypeFromDsType(dstype *string) datapoint.MetricType {
	if dstype == nil {
		return datapoint.Gauge
	}

	m := map[string]datapoint.MetricType{
		"gauge":    datapoint.Gauge,
		"derive":   datapoint.Counter,
		"counter":  datapoint.Counter,
		"absolute": datapoint.Count,
	}
	v, ok := m[*dstype]
	if ok {
		return v
	}
	return datapoint.Gauge
}

func isNilOrEmpty(str *string) bool {
	return str == nil || *str == ""
}

// NewDatapoint creates a new datapoint from collectd's write_http endpoint JSON format
// defaultDimensions are added to the datapoint created, but will be overridden by any dimension
// values in the JSON
func NewDatapoint(point *JSONWriteFormat, index uint, defaultDimensions map[string]string) *datapoint.Datapoint {
	dstype, val, dsname := point.Dstypes[index], point.Values[index], point.Dsnames[index]
	// if you add another  dimension that we read from the json update this number
	const MaxCollectDDims = 6
	dimensions := make(map[string]string, len(defaultDimensions)+MaxCollectDDims)
	for k, v := range defaultDimensions {
		dimensions[k] = v
	}

	metricType := metricTypeFromDsType(dstype)
	metricName, usedParts := getReasonableMetricName(point, index)
	// Don't add empty dimensions
	addIfNotNullOrEmpty(dimensions, "host", true, point.Host)
	addIfNotNullOrEmpty(dimensions, "plugin", true, point.Plugin)
	parseInstanceNameForDimensions(dimensions, "plugin_instance", true, point.PluginInstance)

	_, usedInMetricName := usedParts["type_instance"]
	parseInstanceNameForDimensions(dimensions, "type_instance", !usedInMetricName, point.TypeInstance)

	_, usedInMetricName = usedParts["type"]
	addIfNotNullOrEmpty(dimensions, "type", !usedInMetricName, point.TypeS)

	_, usedInMetricName = usedParts["dsname"]
	addIfNotNullOrEmpty(dimensions, "dsname", !usedInMetricName, dsname)

	timestamp := time.Unix(0, int64(float64(time.Second)**point.Time))
	return datapoint.New(metricName, dimensions, datapoint.NewFloatValue(*val), metricType, timestamp)
}

func addIfNotNullOrEmpty(dimensions map[string]string, key string, cond bool, val *string) {
	if cond && val != nil && *val != "" {
		dimensions[key] = *val
	}
}

// getDimensionsFromName pulls out dimensions out of name in the format name[k=v,f=x]-morename would
// return name-morename and extract dimensions (k,v) and (f,x)
// if we encounter something we don't expect use original.  This function considers dimensions in
// the last, smallest [] in instanceName
func getDimensionsFromName(instanceName string) (string, map[string]string) {
	openBracketIndex := strings.LastIndex(instanceName, "[")
	if openBracketIndex > -1 {
		left := instanceName[:openBracketIndex]
		rest := instanceName[openBracketIndex+1:]
		closeBracketIndex := strings.Index(rest, "]")
		if closeBracketIndex > -1 {
			dimensions := rest[:closeBracketIndex]
			rest = rest[closeBracketIndex+1:]
			pieces := strings.Split(dimensions, ",")
			extraDimensions := make(map[string]string, len(pieces))
			for i := range pieces {
				tokens := strings.Split(pieces[i], "=")
				if len(tokens) != 2 {
					return instanceName, map[string]string{}
				}
				extraDimensions[tokens[0]] = tokens[1]
			}
			return left + rest, extraDimensions
		}
	}
	return instanceName, map[string]string{}
}

func parseInstanceNameForDimensions(dimensions map[string]string, key string, cond bool, val *string) {
	if val == nil {
		return
	}
	instanceName, toAddDims := getDimensionsFromName(*val)

	for k, v := range toAddDims {
		if _, exists := dimensions[k]; !exists {
			addIfNotNullOrEmpty(dimensions, k, true, &v)
		}
	}
	addIfNotNullOrEmpty(dimensions, key, cond, &instanceName)
}

func getReasonableMetricName(point *JSONWriteFormat, index uint) (string, map[string]struct{}) {
	parts := []string{}
	usedParts := make(map[string]struct{})
	if !isNilOrEmpty(point.TypeS) {
		parts = append(parts, *point.TypeS)
		usedParts["type"] = struct{}{}
	}
	if !isNilOrEmpty(point.TypeInstance) {
		instanceName, _ := getDimensionsFromName(*point.TypeInstance)
		parts = append(parts, instanceName)
		usedParts["type_instance"] = struct{}{}
	}
	if !isNilOrEmpty(point.Dsnames[index]) && len(point.Dsnames) > 1 {
		parts = append(parts, *point.Dsnames[index])
		usedParts["dsname"] = struct{}{}
	}
	return strings.Join(parts, "."), usedParts
}
