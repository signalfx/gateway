package metricdeconstructor

import (
	"encoding/json"
	"fmt"
	"strings"

	"errors"

	"github.com/signalfx/golib/datapoint"
)

func split(path string, delimiter string) []string {
	return strings.Split(path, delimiter)
}

type typer struct {
	MetricTypeString string `json:"MetricType"`
	MetricType       *datapoint.MetricType
	P                *configurableDelimiterMetricDeconstructor
}

func (t *typer) verifyType() error {
	if t.MetricTypeString != "" {
		metricType, ok := nameToType[t.MetricTypeString]
		if !ok {
			return fmt.Errorf("cannot parse configured MetricType of %s", t.MetricTypeString)
		}
		t.MetricType = &metricType
	}
	return nil
}

type typeRule struct {
	typer
	StartsWith string `json:"StartsWith"`
	EndsWith   string `json:"EndsWith"`
}

var errTypeRuleNotDefined = errors.New("a TypeRule is defined without a type")
var errTypeRuleIllDefined = errors.New("a TypeRule is defined with neither StartsWith or EndsWith")

func (t *typeRule) verify() error {
	err := t.verifyType()
	if err != nil {
		return err
	}
	if t.MetricType == nil {
		return errTypeRuleNotDefined
	}
	if t.StartsWith == "" && t.EndsWith == "" {
		return errTypeRuleIllDefined
	}
	return nil
}

func (t *typeRule) extractType(line *string) *datapoint.MetricType {
	if (t.StartsWith != "" && strings.HasPrefix(*line, t.StartsWith) || t.StartsWith == "") && (t.EndsWith != "" && strings.HasSuffix(*line, t.EndsWith) || t.EndsWith == "") {
		return t.MetricType

	}
	return nil
}

type configurableDelimiterMetricRule struct {
	typer
	MetricPath           string            `json:"MetricPath"`
	DimensionsMap        string            `json:"DimensionsMap"`
	MetricName           string            `json:"MetricName"`
	AdditionalDimensions map[string]string `json:"Dimensions"`
	MetricPathParts      []matcher
	DimensionsPathParts  []string
	UseCount             int64
}

type matcher interface {
	match(string) bool
}

type starMatcher struct{}

// startMatcher always matches
func (s *starMatcher) match(term string) bool {
	return true
}

type idMatcher struct {
	Term string
}

// idMatcher matches iff the term is exactly equal to s.Term
func (s *idMatcher) match(term string) bool {
	if s.Term == term {
		return true
	}
	return false
}

type notMatcher struct {
	Term string
}

// notMatcher matches iff the term is not exactly equal to s.Term
func (s *notMatcher) match(term string) bool {
	if s.Term != term {
		return true
	}
	return false
}

type termMatcher struct {
	TermOptions []matcher
}

// termMatcher matches if one of the terms within it matches
func (t *termMatcher) match(term string) bool {
	for _, o := range t.TermOptions {
		if o.match(term) {
			return true
		}
	}
	return false
}

func (m *configurableDelimiterMetricRule) newMatchTerm(term string) matcher {
	if term == m.P.Globbing {
		return &starMatcher{}
	}
	terms := split(term, m.P.OrDelimiter)
	termMatchers := make([]matcher, len(terms))
	for i, o := range terms {
		if len(o) > 0 && o[:1] == m.P.NotDelimiter {
			termMatchers[i] = &notMatcher{o[1:]}
		} else {
			termMatchers[i] = &idMatcher{o}
		}
	}
	return &termMatcher{TermOptions: termMatchers}
}

func (m *configurableDelimiterMetricRule) parseMetricPath() {
	if m.MetricPath == "" {
		m.MetricPathParts = make([]matcher, 0)
	} else {
		terms := split(m.MetricPath, m.P.Delimiter)
		m.MetricPathParts = make([]matcher, len(terms))
		for i, term := range terms {
			m.MetricPathParts[i] = m.newMatchTerm(term)
		}
	}
	for i := len(m.MetricPathParts); i < len(m.DimensionsPathParts); i++ {
		m.MetricPathParts = append(m.MetricPathParts, &starMatcher{})
	}
}

func (m *configurableDelimiterMetricRule) String() string {
	return fmt.Sprintf("MetricsPath: %s\nDimensionsMap: %s\nUseCount: %d\nAdditionalDimensions %v\nMetricPathParts %v\nDimensionsPathParts %v\n", m.MetricPath, m.DimensionsMap, m.UseCount, m.AdditionalDimensions, m.MetricPathParts, m.DimensionsPathParts)
}

func (m *configurableDelimiterMetricRule) verify() error {
	m.DimensionsPathParts = split(m.DimensionsMap, m.P.Delimiter)
	m.parseMetricPath()
	if len(m.MetricPathParts) != len(m.DimensionsPathParts) {
		return fmt.Errorf("the MetricPath %s has %d terms but DimensionsMap %s has %d", m.MetricPath, len(m.MetricPathParts), m.DimensionsMap, len(m.DimensionsPathParts))
	}
	found := false
	for _, piece := range m.DimensionsPathParts {
		if piece == m.P.MetricIdentifier {
			found = true
			break
		}
	}
	if !found && m.MetricName == "" {
		return fmt.Errorf("the DimensionsMap does not have a metric specified; use %s to specify the metric name override MetricName", m.P.MetricIdentifier)
	}
	err := m.verifyType()
	if err != nil {
		return err
	}
	return nil
}

func (m *configurableDelimiterMetricRule) extract(metricPieces []string) ([]string, map[string][]string) {
	dims := make(map[string][]string)
	metric := make([]string, 0, 1)

	if m.MetricName != "" {
		metric = append(metric, m.MetricName)
	}

	for i := 0; i < len(metricPieces); i++ {
		if m.MetricPathParts[i].match(metricPieces[i]) {
			if m.DimensionsPathParts[i] == m.P.Ignore {
				continue
			} else if m.DimensionsPathParts[i] == m.P.MetricIdentifier {
				metric = append(metric, metricPieces[i])
			} else {
				_, ok := dims[m.DimensionsPathParts[i]]
				if !ok {
					dims[m.DimensionsPathParts[i]] = make([]string, 0, 1)
				}
				dims[m.DimensionsPathParts[i]] = append(dims[m.DimensionsPathParts[i]], metricPieces[i])
			}
		} else {
			return nil, nil
		}
	}
	return metric, dims
}

func (m *configurableDelimiterMetricRule) extractDimensions(metricPieces []string) (string, map[string]string) {
	metric, dims := m.extract(metricPieces)
	dimensions := make(map[string]string)
	for k, v := range dims {
		dimensions[k] = strings.Join(v, m.P.Delimiter)
	}
	for k, v := range m.AdditionalDimensions {
		_, ok := dimensions[k]
		if !ok {
			dimensions[k] = v
		}
	}
	// precedence is for additional dimensions on the rule over the whole deconstructor
	for k, v := range m.P.AdditionalDimensions {
		_, ok := dimensions[k]
		if !ok {
			dimensions[k] = v
		}
	}
	m.UseCount++
	return strings.Join(metric, m.P.Delimiter), dimensions
}

type configurableDelimiterMetricDeconstructor struct {
	OrDelimiter                 string                             `json:"OrDelimiter"`
	Delimiter                   string                             `json:"Delimiter"`
	Globbing                    string                             `json:"Globbing"`
	Ignore                      string                             `json:"IgnoreDimension"`
	NotDelimiter                string                             `json:"NotDelimiter"`
	MetricIdentifier            string                             `json:"MetricIdentifier"`
	DefaultMetricType           datapoint.MetricType               `json:"DefaultMetricType"`
	MetricRules                 []*configurableDelimiterMetricRule `json:"MetricRules"`
	FallBackDeconstructorName   string                             `json:"FallbackDeconstructor"`
	FallBackDeconstructorConfig string                             `json:"FallbackDeconstructorConfig"`
	AdditionalDimensions        map[string]string                  `json:"Dimensions"`
	FallBackDeconstructor       MetricDeconstructor
	MetricsMap                  map[int][]*configurableDelimiterMetricRule
	TypeRules                   []*typeRule `json:"TypeRules"`
}

func (m *configurableDelimiterMetricDeconstructor) split(path string) []string {
	return split(path, m.Delimiter)
}

func (m *configurableDelimiterMetricDeconstructor) String() string {
	return fmt.Sprintf("ConfigurableDelimiterMetricDeconstructor Metric Rules: %v\nType Rules: %v\n", m.MetricRules, m.TypeRules)
}

func (m *configurableDelimiterMetricDeconstructor) verifyConfig() error {
	allOptions := []string{m.OrDelimiter, m.Delimiter, m.Globbing, m.Ignore, m.MetricIdentifier, m.NotDelimiter}
	for i, vi := range allOptions {
		for j, vj := range allOptions {
			if i != j && vi == vj {
				return fmt.Errorf("overridden option cannot be the same as another: %s", vi)
			}
		}
	}
	return nil
}

func (m *configurableDelimiterMetricDeconstructor) verify() error {
	err := m.verifyConfig()
	if err != nil {
		return err
	}
	m.MetricsMap = make(map[int][]*configurableDelimiterMetricRule)
	for _, t := range m.TypeRules {
		t.P = m
		err := t.verify()
		if err != nil {
			return err
		}
	}
	for _, metric := range m.MetricRules {
		metric.P = m
		err := metric.verify()
		if err != nil {
			return err
		}
		length := len(metric.MetricPathParts)
		maplist, ok := m.MetricsMap[length]
		if !ok {
			maplist = make([]*configurableDelimiterMetricRule, 0)
		}
		m.MetricsMap[length] = append(maplist, metric)
	}

	loader, exists := knownLoaders[m.FallBackDeconstructorName]

	if !exists {
		return fmt.Errorf("unable to load metric deconstructor by the name of %s", m.FallBackDeconstructorName)
	}
	decon, err := loader(m.FallBackDeconstructorConfig)
	if err != nil {
		return err
	}
	m.FallBackDeconstructor = decon

	return nil
}

func (m *configurableDelimiterMetricDeconstructor) getMetricType(metric *configurableDelimiterMetricRule, line *string) datapoint.MetricType {
	if metric.MetricType != nil {
		return *metric.MetricType
	}
	for _, t := range m.TypeRules {
		mt := t.extractType(line)
		if mt != nil {
			return *mt
		}
	}
	return datapoint.Gauge
}

// Parse turns the line into its dimensions, metric type and metric name based on configuration rules defined when constructing the deconstructor
func (m *configurableDelimiterMetricDeconstructor) Parse(originalMetric string) (string, datapoint.MetricType, map[string]string, error) {
	metricPieces := m.split(originalMetric)
	length := len(metricPieces)
	metrics, ok := m.MetricsMap[length]
	if !ok {
		return m.FallBackDeconstructor.Parse(originalMetric)
	}

	for _, metric := range metrics {
		metricName, dimensions := metric.extractDimensions(metricPieces)
		if metricName != "" {
			return metricName, m.getMetricType(metric, &originalMetric), dimensions, nil
		}
	}
	return m.FallBackDeconstructor.Parse(originalMetric)
}

func delimiterJSONLoader(config map[string]interface{}) (MetricDeconstructor, error) {
	var metrics configurableDelimiterMetricDeconstructor
	// a wee bit of a hack, but at least we know it's valid json and it makes the config easier
	jsonString, _ := json.Marshal(config)
	err := json.Unmarshal([]byte(jsonString), &metrics)
	if err != nil {
		return nil, err
	}
	if metrics.OrDelimiter == "" {
		metrics.OrDelimiter = "|"
	}
	if metrics.NotDelimiter == "" {
		metrics.NotDelimiter = "!"
	}
	if metrics.Delimiter == "" {
		metrics.Delimiter = "."
	}
	if metrics.Globbing == "" {
		metrics.Globbing = "*"
	}
	if metrics.Ignore == "" {
		metrics.Ignore = "-"
	}
	if metrics.MetricIdentifier == "" {
		metrics.MetricIdentifier = "%"
	}
	err = metrics.verify()
	if err != nil {
		return nil, err
	}
	return &metrics, nil
}
