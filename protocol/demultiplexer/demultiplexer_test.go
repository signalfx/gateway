package demultiplexer

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	sendTo := datapoint.NewBufferedForwarder(1, 10, "", 1)
	mOrig := New([]datapoint.NamedStreamer{sendTo})
	m, ok := mOrig.(*streamingDemultiplexerImpl)
	assert.Equal(t, true, ok, "Wrong type!")
	assert.Equal(t, "demultiplexer", m.Name(), "Expect one point")
	dpSent := datapoint.NewRelativeTime("metric", map[string]string{}, datapoint.NewIntValue(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	m.Channel() <- dpSent

	// Wait for something to get on the chan
	for atomic.LoadInt64(&m.totalDatapoints) == 0 {
		time.Sleep(time.Millisecond)
	}

	assert.Equal(t, datapoint.NewIntValue(0), m.Stats()[0].Value(), "Expect zero dropped point")
	assert.Equal(t, 4, len(m.Stats()), "Expect three stats")
	m.Channel() <- dpSent

	// Wait for something to get on the chan
	for atomic.LoadInt64(&m.totalDatapoints) == 1 {
		time.Sleep(time.Millisecond)
	}

	for atomic.LoadInt64(&m.droppedPoints[0]) == 0 {
		time.Sleep(time.Millisecond)
	}

	assert.Equal(t, datapoint.NewIntValue(1), m.Stats()[0].Value(), "Expect one dropped point")
	assert.Equal(t, datapoint.NewIntValue(2), m.Stats()[1].Value(), "Expect one dropped point")
	assert.Equal(t, 4, len(m.Stats()), "Expect three stats")
}
