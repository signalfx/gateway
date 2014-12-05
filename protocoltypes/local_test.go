package protocoltypes

import (
	"errors"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewOnHostDatapoint(t *testing.T) {
	hostname, _ := os.Hostname()
	dp := NewOnHostDatapoint("metric", value.NewFloatWire(3.0), com_signalfuse_metrics_protobuf.MetricType_COUNTER)
	assert.Equal(t, hostname, dp.Dimensions()["host"], "Should get source back")

	osXXXHostname = func() (string, error) { return "", errors.New("unable to get hostname") }
	dp = NewOnHostDatapoint("metric", value.NewFloatWire(3.0), com_signalfuse_metrics_protobuf.MetricType_COUNTER)
	assert.Equal(t, "unknown", dp.Dimensions()["host"], "Should get source back")
	osXXXHostname = os.Hostname
}
