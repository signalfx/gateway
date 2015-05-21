package carbon

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/metricproxy/protocol/carbon/metricdeconstructor"
)

type carbonMetadata int

const (
	carbonNative carbonMetadata = iota
)

// NativeCarbonLine inspects the datapoints metadata to see if it has information about the carbon
// source it came from
func NativeCarbonLine(dp *datapoint.Datapoint) (string, bool) {
	s, exists := dp.Meta[carbonNative]
	if exists {
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
		return nil, fmt.Errorf("invalid carbon input line: %s", line)
	}
	originalMetricName := parts[0]
	metricName, mtype, dimensions, err := metricDeconstructor.Parse(originalMetricName)
	if err != nil {
		return nil, err
	}
	metricTime, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid carbon metric time on input line %s: %s", line, err)
	}

	v, err := func() (datapoint.Value, error) {
		metricValueInt, err := strconv.ParseInt(parts[1], 10, 64)
		if err == nil {
			return datapoint.NewIntValue(metricValueInt), nil
		}
		metricValueFloat, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return nil, fmt.Errorf("unable to parse carbon metric value on line %s: %s", line, err)
		}
		return datapoint.NewFloatValue(metricValueFloat), nil
	}()
	if err != nil {
		return nil, err
	}
	return datapoint.NewWithMeta(metricName, dimensions, meta, v, mtype, time.Unix(0, metricTime*int64(time.Second))), nil
}
