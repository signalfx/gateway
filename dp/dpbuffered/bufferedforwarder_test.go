package dpbuffered

import (
	"testing"
	"time"

	"fmt"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/metricproxy/config"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

const numStats = 4

// TODO figure out why this test is flaky, should be > 2, but change to >= 2 so it passes
func TestBufferedForwarderBasic(t *testing.T) {
	ctx := context.Background()
	config := Config{
		BufferSize:         210,
		MaxTotalDatapoints: 1000,
		MaxTotalEvents:     1000,
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
	assert.True(t, len(seen) >= 2, fmt.Sprintf("Points should buffer: %d", len(seen)))
	assert.Equal(t, numStats, len(bf.Stats(map[string]string{})), "Checking returned stats size")
}

// TODO figure out why this test is flaky, should be > 2, but change to >= 2 so it passes
func TestBufferedForwarderBasicEvent(t *testing.T) {
	ctx := context.Background()
	config := Config{
		BufferSize:         210,
		MaxTotalDatapoints: 1000,
		MaxTotalEvents:     1000,
		NumDrainingThreads: 1,
		MaxDrainSize:       1000,
	}
	sendTo := dptest.NewBasicSink()
	bf := NewBufferedForwarder(ctx, config, sendTo)
	defer bf.Close()
	assert.NoError(t, bf.AddEvents(ctx, []*event.Event{}))
	time.Sleep(time.Millisecond)
	for i := 0; i < 100; i++ {
		datas := []*event.Event{
			dptest.E(),
			dptest.E(),
		}
		assert.NoError(t, bf.AddEvents(ctx, datas))
		if i == 0 {
			seen := <-sendTo.EventsChan
			assert.Equal(t, 2, len(seen), "The first send should eventually come back with the first two events")
		}
	}
	// Wait for more events
	seen := <-sendTo.EventsChan
	assert.True(t, len(seen) >= 2, fmt.Sprintf("Events should buffer: %d", len(seen)))
	assert.Equal(t, numStats, len(bf.Stats(map[string]string{})), "Checking returned stats size")
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

func TestBufferedForwarderContextsEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	config := Config{
		BufferSize:         0,
		MaxTotalEvents:     10,
		NumDrainingThreads: 2,
		MaxDrainSize:       1000,
	}

	datas := []*event.Event{
		{},
	}

	sendTo := dptest.NewBasicSink()
	bf := NewBufferedForwarder(ctx, config, sendTo)
	bf.AddEvents(ctx, datas)
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
		case bf.eChan <- datas:
		default:
			break outer
		}
	}
	assert.Equal(t, context.Canceled, bf.AddEvents(canceledContext, datas), "Should escape when passed context canceled")
	cancel()
	assert.Equal(t, context.Canceled, bf.AddEvents(context.Background(), datas), "Should err when parent context canceled")
	bf.stopContext = context.Background()
	assert.Equal(t, context.Canceled, bf.AddEvents(canceledContext, datas), "Should escape when passed context canceled")
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
	assert.Equal(t, ErrDPBufferFull, bf.AddDatapoints(ctx, datas), "With small buffer size, I should error out with a full buffer")
}

func TestBufferedForwarderMaxTotalEvents(t *testing.T) {
	config := Config{
		BufferSize:         15,
		MaxTotalEvents:     7,
		NumDrainingThreads: 1,
		MaxDrainSize:       1000,
	}
	ctx := context.Background()
	sendTo := dptest.NewBasicSink()
	bf := NewBufferedForwarder(ctx, config, sendTo)
	defer bf.Close()

	events := []*event.Event{
		{},
		{},
	}
	for i := 0; i < 100; i++ {
		bf.AddEvents(ctx, events)
	}
	assert.Equal(t, ErrEBufferFull, bf.AddEvents(ctx, events), "With small buffer size, I should error out with a full buffer")
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

	assert.Equal(t, int64(2), c1.BufferSize)
	assert.Equal(t, int64(1), c1.MaxDrainSize)
}
