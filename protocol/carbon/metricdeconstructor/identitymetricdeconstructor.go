package metricdeconstructor

import "github.com/signalfx/golib/v3/datapoint"

// IdentityMetricDeconstructor returns a zero dimension gauge of a metric
type IdentityMetricDeconstructor struct {
}

// Parse returns the zero dimension gauge
func (parser *IdentityMetricDeconstructor) Parse(originalMetric string) (string, datapoint.MetricType, map[string]string, error) {
	return originalMetric, datapoint.Gauge, map[string]string{}, nil
}

func identityLoader(options string) (MetricDeconstructor, error) {
	return &IdentityMetricDeconstructor{}, nil
}
