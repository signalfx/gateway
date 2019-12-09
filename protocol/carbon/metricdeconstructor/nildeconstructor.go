package metricdeconstructor

import (
	"errors"
	"github.com/signalfx/golib/datapoint"
)

// ErrSkipMetric is returned when parsing to indicate skipping
var ErrSkipMetric = errors.New("skip metric")

// NilDeconstructor is a parser that always skips a metric
type NilDeconstructor struct{}

// Parse always returns an error
func (m *NilDeconstructor) Parse(originalMetric string) (string, datapoint.MetricType, map[string]string, error) {
	return "", datapoint.Gauge, nil, ErrSkipMetric
}

func nilLoader(options string) (MetricDeconstructor, error) {
	return &NilDeconstructor{}, nil
}
