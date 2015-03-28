package dpbuffered

import (
	"testing"
	"time"

	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/datapoint/dptest"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestBufferedForwarderBasic(t *testing.T) {
	ctx := context.Background()
	config := Config{
		BufferSize:         210,
		MaxTotalDatapoints: 1000,
		NumDrainingThreads: 1,
		MaxDrainSize:       1000,
	}
	sendTo := dptest.NewBasicSink()
	bf := NewBufferedForwarder(ctx, config, sendTo)
	defer bf.Close()
	assert.NoError(t, bf.AddDatapoints(ctx, []*datapoint.Datapoint{}))
	time.Sleep(time.Millisecond)
	for i := 0; i < 100; i++ {
		datas := []*datapoint.Datapoint{
			{},
			{},
		}
		assert.NoError(t, bf.AddDatapoints(ctx, datas))
		if i == 0 {
			seen := <-sendTo.PointsChan
			assert.Equal(t, 2, len(seen), "The first send should eventually come back with the first two points")
		}
	}
	// Wait for more points
	seen := <-sendTo.PointsChan
	assert.True(t, len(seen) > 2, "Points should buffer")
	assert.Equal(t, 2, len(bf.Stats(map[string]string{})), "Checking returned stats size")
}

func TestBufferedForwarderContexts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	config := Config{
		BufferSize:         0,
		MaxTotalDatapoints: 10,
		NumDrainingThreads: 2,
		MaxDrainSize:       1000,
	}

	datas := []*datapoint.Datapoint{
		{},
	}

	sendTo := dptest.NewBasicSink()
	bf := NewBufferedForwarder(ctx, config, sendTo)
	bf.AddDatapoints(ctx, datas)
	canceledContext, cancelFunc := context.WithCancel(ctx)
	waiter := make(chan struct{})
	go func() {
		cancelFunc()
		<-canceledContext.Done()
		bf.Close()
		close(waiter)
		sendTo.Next()
	}()
	// Wait for this to get drained out

	<-waiter
outer:
	for {
		select {
		case bf.dpChan <- datas:
		default:
			break outer
		}
	}
	assert.Equal(t, context.Canceled, bf.AddDatapoints(canceledContext, datas), "Should escape when passed context canceled")
	cancel()
	assert.Equal(t, context.Canceled, bf.AddDatapoints(context.Background(), datas), "Should err when parent context canceled")
	bf.stopContext = context.Background()
	assert.Equal(t, context.Canceled, bf.AddDatapoints(canceledContext, datas), "Should escape when passed context canceled")
}

func TestBufferedForwarderMaxTotalDatapoints(t *testing.T) {
	config := Config{
		BufferSize:         15,
		MaxTotalDatapoints: 7,
		NumDrainingThreads: 1,
		MaxDrainSize:       1000,
	}
	ctx := context.Background()
	sendTo := dptest.NewBasicSink()
	bf := NewBufferedForwarder(ctx, config, sendTo)
	defer bf.Close()

	datas := []*datapoint.Datapoint{
		{},
		{},
	}
	for i := 0; i < 100; i++ {
		bf.AddDatapoints(ctx, datas)
	}
	assert.Equal(t, ErrBufferFull, bf.AddDatapoints(ctx, datas), "With small buffer size, I should error out with a full buffer")
}

func TestConfigLoad(t *testing.T) {
	c1 := Config{}
	c2 := config.ForwardTo{}
	aOne := uint32(1)
	c2.BufferSize = &aOne
	c2.BufferSize = &aOne
	c2.BufferSize = &aOne
	c2.DrainingThreads = &aOne
	c2.MaxDrainSize = &aOne

	c1.FromConfig(&c2)

	assert.Equal(t, 2, c1.BufferSize)
	assert.Equal(t, 1, c1.MaxDrainSize)
}
