package protocoltypes

import (
	"github.com/golang/glog"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"os"
)

var osXXXHostname = os.Hostname

// NewOnHostDatapoint is like NewSingleNameDataPointWithType but also a source
// of this host
func NewOnHostDatapoint(metric string, value value.DatapointValue,
	metricType com_signalfuse_metrics_protobuf.MetricType) core.Datapoint {
	return NewOnHostDatapointDimensions(metric, value, metricType, map[string]string{})
}

// NewOnHostDatapointDimensions is like NewOnHostDatapoint but also with optional dimensions
func NewOnHostDatapointDimensions(metric string, value value.DatapointValue,
	metricType com_signalfuse_metrics_protobuf.MetricType,
	dimensions map[string]string) core.Datapoint {
	hostname, err := osXXXHostname()
	if err != nil {
		glog.Warningf("Unable to find hostname: %s", err)
		hostname = "unknown"
	}
	dimensions["host"] = hostname
	dimensions["source"] = "proxy"
	return core.NewRelativeTimeDatapoint(metric, dimensions, value, metricType, 0)
}
