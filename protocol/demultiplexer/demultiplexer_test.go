package demultiplexer

import (
	"testing"

	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/metricproxy/dp/dpsink"
	"github.com/signalfx/metricproxy/dp/dptest"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestNew(t *testing.T) {
	ctx := context.Background()
	sendTo1 := dptest.NewBasicSink()
	sendTo2 := dptest.NewBasicSink()
	demux := New([]dpsink.Sink{sendTo1, sendTo2})

	pts := []*datapoint.Datapoint{dptest.DP(), dptest.DP()}
	ctx2, _ := context.WithTimeout(ctx, time.Millisecond)
	assert.Error(t, demux.AddDatapoints(ctx2, pts))
	assert.NoError(t, demux.AddDatapoints(context.Background(), []*datapoint.Datapoint{}))
}
