package collectd

import (
	"strings"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
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
	// events
	Message  *string                `json:"message"`
	Meta     map[string]interface{} `json:"meta"`
	Severity *string                `json:"severity"`
}

var dsTypeToMetricType = map[string]datapoint.MetricType{
	"gauge":    datapoint.Gauge,
	"derive":   datapoint.Counter,
	"counter":  datapoint.Counter,
	"absolute": datapoint.Count,
}

func metricTypeFromDsType(dstype *string) datapoint.MetricType {
	if dstype == nil {
		return datapoint.Gauge
	}

	v, ok := dsTypeToMetricType[*dstype]
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
// Dimensions are pulled out of type_instance, plugin_instance and host in that order of precedence
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

	addIfNotNullOrEmpty(dimensions, "plugin", true, point.Plugin)

	_, usedInMetricName := usedParts["type_instance"]
	parseDimensionsOut(dimensions, usedInMetricName, point.TypeInstance, point.PluginInstance, point.Host)

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

func parseDimensionsOut(dimensions map[string]string, usedInMetricName bool, typeInstance *string, pluginInstance *string, host *string) {
	parseNameForDimensions(dimensions, "type_instance", !usedInMetricName, typeInstance)
	parseNameForDimensions(dimensions, "plugin_instance", true, pluginInstance)
	parseNameForDimensions(dimensions, "host", true, host)
}

// try to pull out dimensions out of name in the format name[k=v,f=x]-morename would
// return name-morename and extract dimensions (k,v) and (f,x)
// if we encounter something we don't expect use original
// this is a bit complicated to avoid allocations, string.split allocates, while slices
// inside same function, do not.
func getDimensionsFromName(val *string) (instanceName string, toAddDims map[string]string) {
	instanceName = *val
	index := strings.Index(*val, "[")
	if index > -1 {
		left := (*val)[:index]
		rest := (*val)[index+1:]
		index = strings.Index(rest, "]")
		if index > -1 {
			working := make(map[string]string)
			dimensions := rest[:index]
			rest = rest[index+1:]
			cindex := strings.Index(dimensions, ",")
			prev := 0
			for {
				if cindex == -1 {
					cindex = len(dimensions)
				}
				if dimensions[prev] == ',' {
					prev++
				}
				piece := dimensions[prev:cindex]
				tindex := strings.Index(piece, "=")
				if tindex == -1 || strings.Index(piece[tindex+1:], "=") > -1 {
					return
				}
				working[piece[:tindex]] = piece[tindex+1:]
				if cindex == len(dimensions) {
					break
				}
				prev = cindex
				cindex = strings.Index(dimensions[:prev], ",")
			}
			toAddDims = working
			instanceName = left + rest
		}
	}
	return
}

func parseNameForDimensions(dimensions map[string]string, key string, cond bool, val *string) {
	instanceName, toAddDims := getDimensionsFromName(val)

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
		instanceName, _ := getDimensionsFromName(point.TypeInstance)
		parts = append(parts, instanceName)
		usedParts["type_instance"] = struct{}{}
	}
	if point.Dsnames != nil && !isNilOrEmpty(point.Dsnames[index]) && len(point.Dsnames) > 1 {
		parts = append(parts, *point.Dsnames[index])
		usedParts["dsname"] = struct{}{}
	}
	return strings.Join(parts, "."), usedParts
}

// NewEvent creates a new event from collectd's write_http endpoint JSON format
// defaultDimensions are added to the event created, but will be overridden by any dimension
// values in the JSON
func NewEvent(e *JSONWriteFormat, defaultDimensions map[string]string) *event.Event {
	// if you add another  dimension that we read from the json update this number
	const MaxCollectDDims = 6
	dimensions := make(map[string]string, len(defaultDimensions)+MaxCollectDDims)
	for k, v := range defaultDimensions {
		dimensions[k] = v
	}

	eventType, usedParts := getReasonableMetricName(e, 0)
	addIfNotNullOrEmpty(dimensions, "plugin", true, e.Plugin)
	parseDimensionsOut(dimensions, true, e.TypeInstance, e.PluginInstance, e.Host)

	_, usedInMetricName := usedParts["type"]
	addIfNotNullOrEmpty(dimensions, "type", !usedInMetricName, e.TypeS)
	properties := make(map[string]interface{}, len(e.Meta)+2)
	for k, v := range e.Meta {
		properties[k] = v
	}
	_, exists := e.Meta["severity"]
	if !exists && e.Severity != nil {
		properties["severity"] = *e.Severity
	}
	_, exists = e.Meta["message"]
	if !exists && e.Message != nil {
		properties["message"] = *e.Message
	}

	timestamp := time.Unix(0, int64(float64(time.Second)**e.Time))
	return event.NewWithProperties(eventType, "COLLECTD", dimensions, properties, timestamp)
}
