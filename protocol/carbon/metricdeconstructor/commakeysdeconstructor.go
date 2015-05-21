package metricdeconstructor

import (
	"fmt"
	"strings"

	"github.com/signalfx/golib/datapoint"
)

type commaKeysLoaderDeconstructor struct {
	colonInKey    bool
	metricTypeDim string
}

var nameToType = map[string]datapoint.MetricType{
	"gauge":              datapoint.Gauge,
	"count":              datapoint.Count,
	"cumulative_counter": datapoint.Counter,
}

func (parser *commaKeysLoaderDeconstructor) keyFromDims(dims map[string]string) datapoint.MetricType {
	if parser.metricTypeDim != "" {
		if metricTypeInDimensions, exists := dims[parser.metricTypeDim]; exists {
			delete(dims, parser.metricTypeDim)
			if metricTypeOfDimension, exists := nameToType[strings.ToLower(metricTypeInDimensions)]; exists {
				return metricTypeOfDimension
			}
		}
	}
	return datapoint.Gauge
}

func (parser *commaKeysLoaderDeconstructor) Parse(originalMetric string) (string, datapoint.MetricType, map[string]string, error) {
	dimensions := map[string]string{}
	parts := strings.SplitN(originalMetric, "[", 2)
	retMtype := datapoint.Gauge
	if len(parts) != 2 {
		return originalMetric, retMtype, dimensions, nil
	}
	if len(parts[1]) == 0 || len(parts[0]) == 0 {
		return originalMetric, retMtype, dimensions, nil
	}
	bracketEndIndex := strings.LastIndex(parts[1], "]")
	if bracketEndIndex == -1 {
		return originalMetric, retMtype, dimensions, nil
	}
	dimensionsPart := parts[1][:bracketEndIndex]
	newMetricName := parts[0] + parts[1][bracketEndIndex+1:]
	tagParts := strings.Split(dimensionsPart, ",")
	for _, tagPart := range tagParts {
		var tagName, tagValue string
		var splitIndex int
		if parser.colonInKey {
			splitIndex = strings.LastIndex(tagPart, ":")
		} else {
			splitIndex = strings.Index(tagPart, ":")
		}
		if splitIndex == -1 {
			continue
		}
		tagName = tagPart[0:splitIndex]
		tagValue = tagPart[splitIndex+1:]
		dimensions[tagName] = tagValue
	}

	retMtype = parser.keyFromDims(dimensions)

	return newMetricName, retMtype, dimensions, nil
}

func commaKeysLoader(options string) (MetricDeconstructor, error) {
	optionParts := strings.Split(options, ",")
	ret := commaKeysLoaderDeconstructor{}
	for _, option := range optionParts {
		if option == "" {
			continue
		}
		if option == "coloninkey" {
			ret.colonInKey = true
			continue
		}
		keyedOptions := strings.Split(option, ":")
		if len(keyedOptions) != 2 {
			return nil, fmt.Errorf("unknown commaKeysLoaderDeconstructor parameter %s", options)
		}
		if keyedOptions[0] == "mtypedim" {
			ret.metricTypeDim = keyedOptions[1]
			continue
		}
		return nil, fmt.Errorf("unknown commaKeysLoaderDeconstructor parameter %s", options)
	}
	return &ret, nil
}
