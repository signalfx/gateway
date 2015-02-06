package carbon

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/datapoint"
	"github.com/signalfuse/signalfxproxy/protocol/carbon/metricdeconstructor"
)

// Native signals an object can be directly converted to a carbon datapoint send
type Native interface {
	ToCarbonLine() string
}

// NativeDatapoint is a datapoint that is carbon ready
type NativeDatapoint interface {
	datapoint.Datapoint
	Native
}

type carbonDatapointImpl struct {
	datapoint.Datapoint
	originalLine string
}

func (carbonDatapoint *carbonDatapointImpl) ToCarbonLine() string {
	return carbonDatapoint.originalLine
}

// NewCarbonDatapoint creates a new datapoint from a line in carbon
func NewCarbonDatapoint(line string, metricDeconstructor metricdeconstructor.MetricDeconstructor) (NativeDatapoint, error) {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid carbon input line: %s", line)
	}
	originalMetricName := parts[0]
	metricName, dimensions, err := metricDeconstructor.Parse(originalMetricName)
	if err != nil {
		return nil, err
	}
	metricTime, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid carbon metric time on input line %s: %s", line, err)
	}

	metricValueInt, err := strconv.ParseInt(parts[1], 10, 64)
	if err == nil {
		return &carbonDatapointImpl{
			datapoint.NewAbsoluteTime(
				metricName, dimensions, datapoint.NewIntValue(metricValueInt),
				com_signalfuse_metrics_protobuf.MetricType_GAUGE,
				time.Unix(0, metricTime*int64(time.Second))),
			line,
		}, nil
	}
	metricValueFloat, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse carbon metric value on line %s: %s", line, err)
	}
	return &carbonDatapointImpl{
		datapoint.NewAbsoluteTime(
			metricName, dimensions, datapoint.NewFloatValue(metricValueFloat),
			com_signalfuse_metrics_protobuf.MetricType_GAUGE,
			time.Unix(0, metricTime*int64(time.Second))),
		line,
	}, nil
}
