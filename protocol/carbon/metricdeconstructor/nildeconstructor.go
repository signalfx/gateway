package metricdeconstructor

import (
	"errors"

	"github.com/signalfx/golib/datapoint"
)

type nilDeconstructor struct{}

// Parse always returns an error
func (m *nilDeconstructor) Parse(originalMetric string) (string, datapoint.MetricType, map[string]string, error) {
	return "", datapoint.Gauge, nil, errors.New("nilDeconstructor always returns an error")
}

func nilLoader(options string) (MetricDeconstructor, error) {
	return &nilDeconstructor{}, nil
}
