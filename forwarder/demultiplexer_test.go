package forwarder

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/stretchr/testify/assert"
)

func TestNewStreamingDatapointDemultiplexer(t *testing.T) {
	sendTo := newBasicBufferedForwarder(1, 10, "", 1)
	mOrig := NewStreamingDatapointDemultiplexer([]core.DatapointStreamingAPI{sendTo})
	m, ok := mOrig.(*streamingDemultiplexerImpl)
	assert.Equal(t, true, ok, "Wrong type!")
	assert.Equal(t, "demultiplexer", m.Name(), "Expect one point")
	dpSent := core.NewRelativeTimeDatapoint("metric", map[string]string{}, value.NewIntWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	m.DatapointsChannel() <- dpSent

	// Wait for something to get on the chan
	for atomic.LoadInt64(&m.totalDatapoints) == 0 {
		time.Sleep(time.Millisecond)
	}

	assert.Equal(t, value.NewIntWire(0), m.GetStats()[0].Value(), "Expect zero dropped point")
	assert.Equal(t, 3, len(m.GetStats()), "Expect three stats")
	m.DatapointsChannel() <- dpSent

	// Wait for something to get on the chan
	for atomic.LoadInt64(&m.totalDatapoints) == 1 {
		time.Sleep(time.Millisecond)
	}

	for atomic.LoadInt64(&m.droppedPoints[0]) == 0 {
		time.Sleep(time.Millisecond)
	}

	assert.Equal(t, value.NewIntWire(1), m.GetStats()[0].Value(), "Expect one dropped point")
	assert.Equal(t, value.NewIntWire(2), m.GetStats()[1].Value(), "Expect one dropped point")
	assert.Equal(t, 3, len(m.GetStats()), "Expect three stats")
}
