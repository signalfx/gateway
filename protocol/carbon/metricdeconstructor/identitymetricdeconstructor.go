package metricdeconstructor

type identityMetricDeconstructor struct {
}

func (parser *identityMetricDeconstructor) Parse(originalMetric string) (string, map[string]string, error) {
	return originalMetric, map[string]string{}, nil
}

func identityLoader(options string) (MetricDeconstructor, error) {
	return &identityMetricDeconstructor{}, nil
}
