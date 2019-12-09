package carbon

import (
	"strconv"
	"strings"
	"time"

	"github.com/signalfx/gateway/protocol/carbon/metricdeconstructor"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/errors"
)

type carbonMetadata int

const (
	carbonNative carbonMetadata = iota
)

// NativeCarbonLine inspects the datapoints metadata to see if it has information about the carbon
// source it came from
func NativeCarbonLine(dp *datapoint.Datapoint) (string, bool) {
	if s, exists := dp.Meta[carbonNative]; exists {
		return s.(string), true
	}
	return "", false
}

// NewCarbonDatapoint creates a new datapoint from a line in carbon and injects into the datapoint
// metadata about the original line.
func NewCarbonDatapoint(line string, metricDeconstructor metricdeconstructor.MetricDeconstructor) (*datapoint.Datapoint, error) {
	parts := strings.SplitN(line, " ", 3)
	meta := map[interface{}]interface{}{
		carbonNative: line,
	}
	if len(parts) != 3 {
		return nil, errors.Errorf("Note: Gateway does not support pickle format: invalid carbon input line: %s", line)
	}
	originalMetricName := parts[0]
	metricName, mtype, dimensions, err := metricDeconstructor.Parse(originalMetricName)

	if err == metricdeconstructor.ErrSkipMetric {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	metricTime, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return nil, errors.Annotatef(err, "invalid carbon metric time on input line %s", line)
	}

	v, err := func() (datapoint.Value, error) {
		metricValueInt, err := strconv.ParseInt(parts[1], 10, 64)
		if err == nil {
			return datapoint.NewIntValue(metricValueInt), nil
		}
		metricValueFloat, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return nil, errors.Annotatef(err, "unable to parse carbon metric value on line %s", line)
		}
		return datapoint.NewFloatValue(metricValueFloat), nil
	}()
	if err != nil {
		return nil, err
	}
	return datapoint.NewWithMeta(metricName, dimensions, meta, v, mtype, time.Unix(0, int64(metricTime*1000)*int64(time.Millisecond))), nil
}
