package demultiplexer

import (
	"testing"

	"time"

	"context"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/trace"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	ctx := context.Background()
	sendTo1 := dptest.NewBasicSink()
	sendTo2 := dptest.NewBasicSink()
	sendTo3 := dptest.NewBasicSink()
	demux := Demultiplexer{
		DatapointSinks: []dpsink.DSink{sendTo1},
		EventSinks:     []dpsink.ESink{sendTo2},
		TraceSinks:     []trace.Sink{sendTo3},
	}

	pts := []*datapoint.Datapoint{dptest.DP(), dptest.DP()}
	es := []*event.Event{dptest.E(), dptest.E()}
	traces := []*trace.Span{{}}
	ctx2, cancelFunc := context.WithTimeout(ctx, time.Millisecond)
	assert.Error(t, demux.AddDatapoints(ctx2, pts))
	assert.Error(t, demux.AddEvents(ctx2, es))
	assert.Error(t, demux.AddSpans(ctx2, traces))
	assert.NoError(t, demux.AddDatapoints(context.Background(), []*datapoint.Datapoint{}))
	assert.NoError(t, demux.AddEvents(context.Background(), []*event.Event{}))
	assert.NoError(t, demux.AddSpans(context.Background(), []*trace.Span{}))
	cancelFunc()
}
