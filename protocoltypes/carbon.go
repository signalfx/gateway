package protocoltypes

import (
	"fmt"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"strconv"
	"strings"
	"time"
)

// CarbonReady signals an object can be directly converted to a carbon datapoint send
type CarbonReady interface {
	ToCarbonLine() string
}

// CarbonReadyDatapoint is a datapoint that is carbon ready
type CarbonReadyDatapoint interface {
	core.Datapoint
	CarbonReady
}

type carbonDatapointImpl struct {
	core.Datapoint
	originalLine string
}

func (carbonDatapoint *carbonDatapointImpl) ToCarbonLine() string {
	return carbonDatapoint.originalLine
}

// NewCarbonDatapoint creates a new datapoint from a line in carbon
func NewCarbonDatapoint(line string) (core.Datapoint, error) {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid carbon input line: %s", line)
	}
	metricName := parts[0]
	metricTime, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid carbon metric time on input line %s: %s", line, err)
	}
	metricValueInt, err := strconv.ParseInt(parts[1], 10, 64)
	if err == nil {
		return &carbonDatapointImpl{
			core.NewAbsoluteTimeDatapoint(
				metricName, map[string]string{}, value.NewIntWire(metricValueInt),
				com_signalfuse_metrics_protobuf.MetricType_GAUGE,
				time.Unix(0, metricTime*int64(time.Millisecond))),
			line,
		}, nil
	}
	metricValueFloat, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse carbon metric value on line %s: %s", line, err)
	}
	return &carbonDatapointImpl{
		core.NewAbsoluteTimeDatapoint(
			metricName, map[string]string{}, value.NewFloatWire(metricValueFloat),
			com_signalfuse_metrics_protobuf.MetricType_GAUGE,
			time.Unix(0, metricTime*int64(time.Millisecond))),
		line,
	}, nil
}
