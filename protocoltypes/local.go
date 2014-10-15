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
func NewOnHostDatapoint(metric string, value value.DatapointValue, metricType com_signalfuse_metrics_protobuf.MetricType) core.Datapoint {
	hostname, err := osXXXHostname()
	if err != nil {
		glog.Warningf("Unable to find hostname: %s", err)
		hostname = "unknown"
	}
	return core.NewRelativeTimeDatapoint(metric, map[string]string{"sf_source": hostname}, value, metricType, 0)
}
