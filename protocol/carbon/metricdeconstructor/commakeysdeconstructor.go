package metricdeconstructor

import (
	"fmt"
	"strings"
)

type commaKeysLoaderDeconstructor struct {
	colonInKey bool
}

func (parser *commaKeysLoaderDeconstructor) Parse(originalMetric string) (string, map[string]string, error) {
	dimensions := map[string]string{}
	parts := strings.SplitN(originalMetric, "[", 2)
	if len(parts) != 2 {
		return originalMetric, dimensions, nil
	}
	if len(parts[1]) == 0 || len(parts[0]) == 0 {
		return originalMetric, dimensions, nil
	}
	bracketEndIndex := strings.LastIndex(parts[1], "]")
	if bracketEndIndex == -1 {
		return originalMetric, dimensions, nil
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
	return newMetricName, dimensions, nil
}

func commaKeysLoader(options string) (MetricDeconstructor, error) {
	if options != "" && options != "coloninkey" {
		return nil, fmt.Errorf("unknown commaKeysLoaderDeconstructor parameter %s", options)
	}
	return &commaKeysLoaderDeconstructor{
		colonInKey: options == "coloninkey",
	}, nil
}
