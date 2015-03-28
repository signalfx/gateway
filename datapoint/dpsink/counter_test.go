package dpsink

import (
	"errors"
	"testing"
	"time"

	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/datapoint/dptest"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestCounterSink(t *testing.T) {
	dps := []*datapoint.Datapoint{
		{},
		{},
	}
	ctx := context.Background()
	bs := dptest.NewBasicSink()
	count := Counter{}
	middleSink := count.SinkMiddleware(bs)
	go func() {
		// Allow time for us to get in the middle of a call
		time.Sleep(time.Millisecond)
		assert.Equal(t, 1, count.CallsInFlight, "After a sleep, should be in flight")
		datas := <-bs.PointsChan
		assert.Equal(t, 2, len(datas), "Original datas should be sent")
	}()
	middleSink.AddDatapoints(ctx, dps)
	assert.Equal(t, 0, count.CallsInFlight, "Call is finished")
	assert.Equal(t, 0, count.TotalProcessErrors, "No errors so far (see above)")
	assert.Equal(t, 6, len(count.Stats(map[string]string{})), "Just checking stats len()")

	bs.RetError(errors.New("nope"))
	middleSink.AddDatapoints(ctx, dps)
	assert.Equal(t, 1, count.TotalProcessErrors, "Error should be sent through")
}
