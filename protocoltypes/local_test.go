package protocoltypes

import (
	"github.com/cep21/gohelpers/a"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/vektra/errors"
	"os"
	"testing"
)

func TestNewOnHostDatapoint(t *testing.T) {
	hostname, _ := os.Hostname()
	dp := NewOnHostDatapoint("metric", value.NewFloatWire(3.0), com_signalfuse_metrics_protobuf.MetricType_COUNTER)
	a.ExpectEquals(t, hostname, dp.Dimensions()["sf_source"], "Should get source back")

	osXXXHostname = func() (string, error) { return "", errors.New("unable to get hostname") }
	dp = NewOnHostDatapoint("metric", value.NewFloatWire(3.0), com_signalfuse_metrics_protobuf.MetricType_COUNTER)
	a.ExpectEquals(t, "unknown", dp.Dimensions()["sf_source"], "Should get source back")
	osXXXHostname = os.Hostname
}
