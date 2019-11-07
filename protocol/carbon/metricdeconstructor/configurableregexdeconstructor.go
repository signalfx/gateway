package metricdeconstructor

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/signalfx/golib/v3/datapoint"
)

type configurableRegexMetricRule struct {
	typer
	Regex                string            `json:"Regex"`
	MetricName           string            `json:"MetricName"`
	AdditionalDimensions map[string]string `json:"AdditionalDimensions"`
	MetricTypeString     string            `json:"MetricType"`
	MetricType           datapoint.MetricType
	Compiled             *regexp.Regexp
	UseCount             int64
}

type configurableRegexMetricDeconstructor struct {
	MetricRules                 []*configurableRegexMetricRule `json:"MetricRules"`
	FallBackDeconstructorName   string                         `json:"FallbackDeconstructor"`
	FallBackDeconstructorConfig string                         `json:"FallbackDeconstructorConfig"`
	AdditionalDimensions        map[string]string              `json:"Dimensions"`
	FallBackDeconstructor       MetricDeconstructor
}

func regexJSONLoader(config map[string]interface{}) (MetricDeconstructor, error) {
	var metrics configurableRegexMetricDeconstructor
	// a wee bit of a hack, but at least we know it's valid json and it makes the config easier
	jsonString, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(jsonString, &metrics)
	if err != nil {
		return nil, err
	}

	for _, metricRegex := range metrics.MetricRules {
		if metricRegex.MetricTypeString != "" {
			metricType, ok := nameToType[metricRegex.MetricTypeString]
			if !ok {
				return nil, fmt.Errorf("cannot parse configured MetricType of %s", metricRegex.MetricTypeString)
			}
			metricRegex.MetricType = metricType
		}

		metricRegex.Compiled, err = regexp.Compile(metricRegex.Regex)
		if err != nil {
			return nil, err
		}
	}

	loader, exists := knownLoaders[metrics.FallBackDeconstructorName]

	if !exists {
		return nil, fmt.Errorf("unable to load metric deconstructor by the name of %s", metrics.FallBackDeconstructorName)
	}
	decon, err := loader(metrics.FallBackDeconstructorConfig)
	if err != nil {
		return nil, err
	}
	metrics.FallBackDeconstructor = decon

	return &metrics, nil
}

// Parse turns the line into its dimensions, metric type and metric name based on configuration rules defined when constructing the deconstructor
func (m *configurableRegexMetricDeconstructor) Parse(originalMetric string) (string, datapoint.MetricType, map[string]string, error) {
	for _, metric := range m.MetricRules {
		if metric.Compiled.MatchString(originalMetric) {
			ms := metric.Compiled.FindStringSubmatch(originalMetric)
			nms := metric.Compiled.SubexpNames()
			dimensions := map[string]string{}
			metricNamePieces := []string{}
			metricNameLookup := map[string]string{}

			for i := 1; i < len(ms); i++ {
				if strings.HasPrefix(nms[i], "sf_metric") {
					metricNamePieces = append(metricNamePieces, nms[i])
					metricNameLookup[nms[i]] = ms[i]
				} else {
					dimensions[nms[i]] = ms[i]
				}
			}
			for k, v := range metric.AdditionalDimensions {
				dimensions[k] = v
			}

			sort.Strings(metricNamePieces)
			metricName := []string{metric.MetricName}
			for _, piece := range metricNamePieces {
				metricName = append(metricName, metricNameLookup[piece])
			}
			actualMetricName := strings.Join(metricName, "")
			if actualMetricName == "" {
				actualMetricName = originalMetric
			}
			return actualMetricName, metric.MetricType, dimensions, nil
		}
	}

	return m.FallBackDeconstructor.Parse(originalMetric)
}
