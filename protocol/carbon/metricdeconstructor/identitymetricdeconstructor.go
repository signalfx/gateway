package metricdeconstructor

import "github.com/signalfx/golib/datapoint"

type identityMetricDeconstructor struct {
}

func (parser *identityMetricDeconstructor) Parse(originalMetric string) (string, datapoint.MetricType, map[string]string, error) {
	return originalMetric, datapoint.Gauge, map[string]string{}, nil
}

func identityLoader(options string) (MetricDeconstructor, error) {
	return &identityMetricDeconstructor{}, nil
}
