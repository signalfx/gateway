package demultiplexer

import (
	"testing"

	"time"

	"context"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/datapoint/dpsink"
	"github.com/signalfx/golib/v3/datapoint/dptest"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/log"
	"github.com/signalfx/golib/v3/pointer"
	"github.com/signalfx/golib/v3/sfxclient"
	"github.com/signalfx/golib/v3/trace"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, sfxclient.TokenHeaderName, "foo")
	sendTo1 := dptest.NewBasicSink()
	sendTo2 := dptest.NewBasicSink()
	sendTo3 := dptest.NewBasicSink()
	c := &log.Counter{}
	second := 100 * time.Microsecond
	demux := Demultiplexer{
		DatapointSinks: []dpsink.DSink{sendTo1},
		EventSinks:     []dpsink.ESink{sendTo2},
		TraceSinks:     []trace.Sink{sendTo2, sendTo3},
		Logger:         c,
		LateDuration:   &second,
		FutureDuration: &second,
	}

	late := time.Now().Add(-time.Second)
	future := time.Now().Add(time.Second)
	pts := []*datapoint.Datapoint{dptest.DP(), dptest.DP()}
	pts[0].Timestamp = late
	pts[1].Timestamp = future
	es := []*event.Event{dptest.E(), dptest.E()}
	es[0].Timestamp = late
	es[1].Timestamp = future
	traces := []*trace.Span{{Timestamp: pointer.Int64(late.UnixNano() / 1000), Tags: map[string]string{"foo": "bar"}}, {Timestamp: pointer.Int64(future.UnixNano() / 1000)}}
	ctx2, cancelFunc := context.WithTimeout(ctx, time.Millisecond)
	assert.Error(t, demux.AddDatapoints(ctx2, pts))
	assert.Error(t, demux.AddEvents(ctx2, es))
	assert.Error(t, demux.AddSpans(ctx2, traces))
	assert.Error(t, demux.AddDatapoints(ctx2, pts[1:]))
	assert.Error(t, demux.AddEvents(ctx2, es[1:]))
	assert.Error(t, demux.AddSpans(ctx2, traces[1:]))
	assert.Equal(t, c.Count, int64(9))
	assert.Equal(t, demux.stats.futureDps, int64(2))
	assert.Equal(t, demux.stats.lateDps, int64(1))
	assert.Equal(t, demux.stats.futureEvents, int64(2))
	assert.Equal(t, demux.stats.lateEvents, int64(1))
	assert.Equal(t, demux.stats.futureSpans, int64(2))
	assert.Equal(t, demux.stats.lateSpans, int64(1))
	assert.Equal(t, len(demux.Datapoints()), 6)
	assert.NoError(t, demux.AddDatapoints(context.Background(), []*datapoint.Datapoint{}))
	assert.NoError(t, demux.AddEvents(context.Background(), []*event.Event{}))
	assert.NoError(t, demux.AddSpans(context.Background(), []*trace.Span{}))
	cancelFunc()
}
