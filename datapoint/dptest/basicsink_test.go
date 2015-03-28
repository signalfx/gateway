package dptest

import (
	"errors"
	"testing"

	"github.com/signalfx/metricproxy/datapoint"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestBasicSink(t *testing.T) {
	b := NewBasicSink()
	b.PointsChan = make(chan []*datapoint.Datapoint, 2)
	assert.NoError(t, b.AddDatapoints(context.Background(), []*datapoint.Datapoint{}))
	assert.Equal(t, 1, len(b.PointsChan))
}

func TestBasicSinkErr(t *testing.T) {
	b := NewBasicSink()
	b.RetError(errors.New("nope"))
	assert.Error(t, b.AddDatapoints(context.Background(), []*datapoint.Datapoint{}))
}

func TestContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	progressChan := make(chan struct{})
	b := NewBasicSink()
	go func() {
		cancel()
		close(progressChan)
	}()
	assert.Equal(t, context.Canceled, b.AddDatapoints(ctx, []*datapoint.Datapoint{}))
	<-progressChan
}

func TestNext(t *testing.T) {
	ctx := context.Background()
	b := NewBasicSink()
	dp := DP()
	go b.AddDatapoints(ctx, []*datapoint.Datapoint{dp})
	dpSeen := b.Next()
	assert.Equal(t, dpSeen, dp)

	go b.AddDatapoints(ctx, []*datapoint.Datapoint{dp, dp})
	assert.Panics(t, func() {
		b.Next()
	})
}
