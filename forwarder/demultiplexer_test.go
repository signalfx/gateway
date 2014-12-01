package forwarder

import (
	"github.com/cep21/gohelpers/a"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"testing"
	"time"
)

func TestNewStreamingDatapointDemultiplexer(t *testing.T) {
	sendTo := newBasicBufferedForwarder(1, 10, "", 1)
	mOrig := NewStreamingDatapointDemultiplexer([]core.DatapointStreamingAPI{sendTo})
	m, ok := mOrig.(*streamingDemultiplexerImpl)
	a.ExpectEquals(t, true, ok, "Wrong type!")
	a.ExpectEquals(t, "demultiplexer", m.Name(), "Expect one point")
	dpSent := core.NewRelativeTimeDatapoint("metric", map[string]string{}, value.NewIntWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	m.DatapointsChannel() <- dpSent

	// Wait for something to get on the chan
	for m.totalDatapoints == 0 {
		time.Sleep(time.Millisecond)
	}

	a.ExpectEquals(t, value.NewIntWire(0), m.GetStats()[0].Value(), "Expect zero dropped point")
	a.ExpectEquals(t, 3, len(m.GetStats()), "Expect three stats")
	m.DatapointsChannel() <- dpSent

	// Wait for something to get on the chan
	for m.totalDatapoints == 1 {
		time.Sleep(time.Millisecond)
	}
	a.ExpectEquals(t, value.NewIntWire(1), m.GetStats()[0].Value(), "Expect one dropped point")
	a.ExpectEquals(t, value.NewIntWire(2), m.GetStats()[1].Value(), "Expect one dropped point")
	a.ExpectEquals(t, 3, len(m.GetStats()), "Expect three stats")
}
