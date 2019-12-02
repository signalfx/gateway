package metricdeconstructor

import (
	"errors"
	"github.com/signalfx/golib/datapoint"
)

var SkipMetricErr = errors.New("skip metric")

type nilDeconstructor struct{}

// Parse always returns an error
func (m *nilDeconstructor) Parse(originalMetric string) (string, datapoint.MetricType, map[string]string, error) {
	return "", datapoint.Gauge, nil, SkipMetricErr
}

func nilLoader(options string) (MetricDeconstructor, error) {
	return &nilDeconstructor{}, nil
}
