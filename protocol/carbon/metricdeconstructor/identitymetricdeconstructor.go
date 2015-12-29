package metricdeconstructor

import "github.com/signalfx/golib/datapoint"

type IdentityMetricDeconstructor struct {
}

func (parser *IdentityMetricDeconstructor) Parse(originalMetric string) (string, datapoint.MetricType, map[string]string, error) {
	return originalMetric, datapoint.Gauge, map[string]string{}, nil
}

func identityLoader(options string) (MetricDeconstructor, error) {
	return &IdentityMetricDeconstructor{}, nil
}
