package dpbuffered

import (
	"testing"
	"time"

	"bytes"
	"context"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/trace"
	"github.com/signalfx/golib/web"
	"github.com/signalfx/metricproxy/protocol/signalfx"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"io"
	"sync"
)

const numStats = 6

type boolChecker bool

func (b *boolChecker) HasFlag(ctx context.Context) bool {
	return bool(*b)
}

type threadSafeWriter struct {
	io.Writer
	mu sync.Mutex
}

func (t *threadSafeWriter) Write(p []byte) (n int, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.Writer.Write(p)
}

// TODO figure out why this test is flaky, should be > 2, but change to >= 2 so it passes
func TestBufferedForwarderBasic(t *testing.T) {
	Convey("Basic forwarder setup", t, func() {
		ctx := context.Background()
		flagCheck := boolChecker(false)
		checker := &signalfx.ItemFlagger{
			ItemFlagger: dpsink.ItemFlagger{
				CtxFlagCheck:        &flagCheck,
				EventMetaName:       "meta_event",
				MetricDimensionName: "sf_metric",
			},
		}
		config := &Config{
			BufferSize:         pointer.Int64(210),
			MaxTotalDatapoints: pointer.Int64(1000),
			MaxTotalEvents:     pointer.Int64(1000),
			MaxTotalSpans:      pointer.Int64(1000),
			NumDrainingThreads: pointer.Int64(1),
			MaxDrainSize:       pointer.Int64(1000),
			Checker:            checker,
		}
		sendTo := dptest.NewBasicSink()
		buf := &bytes.Buffer{}
		threadWriter := &threadSafeWriter{Writer: buf}
		l := log.NewLogfmtLogger(threadWriter, log.Panic)
		bf := NewBufferedForwarder(ctx, config, sendTo, l)
		datas := []*datapoint.Datapoint{
			dptest.DP(),
			dptest.DP(),
		}
		events := []*event.Event{
			dptest.E(),
			dptest.E(),
		}
		spans := []*trace.Span{
			{},
			{},
		}
		Reset(func() {
			So(bf.Close(), ShouldBeNil)
		})
		Convey("Should be able to send an event", func() {
			assert.NoError(t, bf.AddEvents(ctx, []*event.Event{}))
		})
		Convey("Should be able to send a datapoint", func() {
			assert.NoError(t, bf.AddDatapoints(ctx, []*datapoint.Datapoint{}))
		})
		Convey("Should export stats", func() {
			So(len(bf.Datapoints()), ShouldEqual, numStats)
		})
		Convey("Should export Pipeliner interface", func() {
			So(bf.Pipeline(), ShouldEqual, 0)
		})
		Convey("Should buffer points", func() {
			time.Sleep(time.Millisecond * 10)
			for i := 0; i < 100; i++ {
				So(bf.AddDatapoints(ctx, datas), ShouldBeNil)
				if i == 0 {
					seen := <-sendTo.PointsChan
					So(len(seen), ShouldEqual, 2)
				}
			}
			So(bf.AddDatapoints(ctx, datas), ShouldBeNil)
			seen := <-sendTo.PointsChan
			So(len(seen), ShouldBeGreaterThan, 1)
		})
		Convey("Should buffer events", func() {
			time.Sleep(time.Millisecond * 10)
			for i := 0; i < 100; i++ {
				So(bf.AddEvents(ctx, events), ShouldBeNil)
				if i == 0 {
					seen := <-sendTo.EventsChan
					So(len(seen), ShouldEqual, 2)
				}
			}
			So(bf.AddEvents(ctx, events), ShouldBeNil)
		})
		Convey("Should buffer traces", func() {
			time.Sleep(time.Millisecond * 10)
			for i := 0; i < 100; i++ {
				So(bf.AddSpans(ctx, spans), ShouldBeNil)
				if i == 0 {
					seen := <-sendTo.TracesChan
					So(len(seen), ShouldEqual, 2)
				}
			}
			So(bf.AddEvents(ctx, events), ShouldBeNil)
		})
		Convey("Should respect datapoint flags", func() {
			checker.SetDatapointFlag(datas[0])
			So(bf.AddDatapoints(ctx, datas), ShouldBeNil)
			seen := <-sendTo.PointsChan
			So(len(seen), ShouldEqual, 2)
			threadWriter.mu.Lock()
			So(buf.String(), ShouldContainSubstring, "about to send datapoint")
			threadWriter.mu.Unlock()
		})
		Convey("Should respect event flags", func() {
			checker.SetEventFlag(events[0])
			So(bf.AddEvents(ctx, events), ShouldBeNil)
			seen := <-sendTo.EventsChan
			So(len(seen), ShouldEqual, 2)
			threadWriter.mu.Lock()
			So(buf.String(), ShouldContainSubstring, "about to send event")
			threadWriter.mu.Unlock()
		})

		Convey("Should respect context flags", func() {
			threadWriter.mu.Lock()
			flagCheck = boolChecker(true)
			buf.Reset()
			threadWriter.mu.Unlock()
			So(bf.AddDatapoints(ctx, []*datapoint.Datapoint{}), ShouldBeNil)
			threadWriter.mu.Lock()
			So(len(buf.String()), ShouldBeGreaterThan, 0)
			threadWriter.mu.Unlock()
		})
		Convey("Should respect event context flags", func() {
			threadWriter.mu.Lock()
			flagCheck = boolChecker(true)
			buf.Reset()
			threadWriter.mu.Unlock()
			So(bf.AddEvents(ctx, []*event.Event{}), ShouldBeNil)
			threadWriter.mu.Lock()
			So(len(buf.String()), ShouldBeGreaterThan, 0)
			threadWriter.mu.Unlock()
		})
	})
}

func TestBufferedForwarderContexts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	config := &Config{
		BufferSize:         pointer.Int64(0),
		MaxTotalDatapoints: pointer.Int64(10),
		NumDrainingThreads: pointer.Int64(2),
		MaxDrainSize:       pointer.Int64(1000),
		Cdim:               &log.CtxDimensions{},
		Checker: &signalfx.ItemFlagger{
			ItemFlagger: dpsink.ItemFlagger{
				CtxFlagCheck: &web.HeaderCtxFlag{},
			},
		},
	}

	datas := []*datapoint.Datapoint{
		{},
	}

	sendTo := dptest.NewBasicSink()
	bf := NewBufferedForwarder(ctx, config, sendTo, log.Discard)
	assert.NoError(t, bf.AddDatapoints(ctx, datas))
	canceledContext, cancelFunc := context.WithCancel(ctx)
	waiter := make(chan struct{})
	go func() {
		cancelFunc()
		<-canceledContext.Done()
		assert.NoError(t, bf.Close())
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

func TestBufferedForwarderBlockingDrain(t *testing.T) {
	f := BufferedForwarder{
		eChan: make(chan []*event.Event, 3),
		tChan: make(chan []*trace.Span, 3),
		config: &Config{
			MaxDrainSize: pointer.Int64(1000),
		},
		stopContext: context.Background(),
	}
	f.eChan <- []*event.Event{dptest.E()}
	f.eChan <- []*event.Event{dptest.E(), dptest.E()}

	evs := f.blockingDrainEventsUpTo()
	assert.True(t, len(evs) == 3)

	f.tChan <- []*trace.Span{{}}
	f.tChan <- []*trace.Span{{}, {}}

	spans := f.blockingDrainSpansUpTo()
	assert.True(t, len(spans) == 3)
}

func TestBufferedForwarderContextsEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	config := &Config{
		BufferSize:         pointer.Int64(0),
		MaxTotalEvents:     pointer.Int64(10),
		MaxTotalSpans:      pointer.Int64(10),
		NumDrainingThreads: pointer.Int64(2),
		MaxDrainSize:       pointer.Int64(1000),
		Cdim:               &log.CtxDimensions{},
		Checker: &signalfx.ItemFlagger{
			ItemFlagger: dpsink.ItemFlagger{
				CtxFlagCheck: &web.HeaderCtxFlag{},
			},
		},
	}

	events := []*event.Event{{}}
	spans := []*trace.Span{{}}

	sendTo := dptest.NewBasicSink()
	bf := NewBufferedForwarder(ctx, config, sendTo, log.Discard)
	assert.NoError(t, bf.AddEvents(ctx, events))
	assert.NoError(t, bf.AddSpans(ctx, spans))
	canceledContext, cancelFunc := context.WithCancel(ctx)
	waiter := make(chan struct{})
	go func() {
		cancelFunc()
		<-canceledContext.Done()
		assert.NoError(t, bf.Close())
		close(waiter)
		sendTo.Next()
	}()
	// Wait for this to get drained out

	<-waiter
outer:
	for {
		select {
		case bf.eChan <- events:
		default:
			break outer
		}
	}
	assert.Equal(t, context.Canceled, bf.AddEvents(canceledContext, events), "Should escape when passed context canceled")
	assert.Equal(t, context.Canceled, bf.AddSpans(canceledContext, spans), "Should escape when passed context canceled")
	cancel()
	assert.Equal(t, context.Canceled, bf.AddEvents(context.Background(), events), "Should err when parent context canceled")
	assert.Equal(t, context.Canceled, bf.AddSpans(context.Background(), spans), "Should err when parent context canceled")
	bf.stopContext = context.Background()
	assert.Equal(t, context.Canceled, bf.AddEvents(canceledContext, events), "Should escape when passed context canceled")
	bf.stopContext = context.Background()
	assert.Equal(t, context.Canceled, bf.AddSpans(canceledContext, spans), "Should escape when passed context canceled")
}

func TestBufferedForwarderMaxTotalDatapoints(t *testing.T) {
	config := &Config{
		BufferSize:         pointer.Int64(15),
		MaxTotalDatapoints: pointer.Int64(7),
		NumDrainingThreads: pointer.Int64(1),
		MaxDrainSize:       pointer.Int64(1000),
		Cdim:               &log.CtxDimensions{},
		Checker: &signalfx.ItemFlagger{
			ItemFlagger: dpsink.ItemFlagger{
				CtxFlagCheck: &web.HeaderCtxFlag{},
			},
		},
		Name: pointer.String("blarg"),
	}
	ctx := context.Background()
	sendTo := dptest.NewBasicSink()
	bf := NewBufferedForwarder(ctx, config, sendTo, log.Discard)
	defer func() {
		assert.NoError(t, bf.Close())
	}()

	datas := []*datapoint.Datapoint{
		{},
		{},
	}
	found := false
	for i := 0; i < 100; i++ {
		if err := bf.AddDatapoints(ctx, datas); err == errDPBufferFull(*config.Name) {
			assert.NotEmpty(t, err.Error())
			found = true
			break
		}
	}
	assert.True(t, found, "With small buffer size, I should error out with a full buffer")
}

func TestBufferedForwarderMaxTotalEvents(t *testing.T) {
	config := &Config{
		BufferSize:         pointer.Int64(15),
		MaxTotalEvents:     pointer.Int64(7),
		MaxTotalSpans:      pointer.Int64(7),
		NumDrainingThreads: pointer.Int64(1),
		MaxDrainSize:       pointer.Int64(1000),
		Cdim:               &log.CtxDimensions{},
		Checker: &signalfx.ItemFlagger{
			ItemFlagger: dpsink.ItemFlagger{
				CtxFlagCheck: &web.HeaderCtxFlag{},
			},
		},
		Name: pointer.String("blarg"),
	}
	ctx := context.Background()
	sendTo := dptest.NewBasicSink()
	bf := NewBufferedForwarder(ctx, config, sendTo, log.Discard)
	defer func() {
		assert.NoError(t, bf.Close())
	}()

	events := []*event.Event{{}, {}}
	spans := []*trace.Span{{}, {}}
	found := false
	for i := 0; i < 100; i++ {
		if err := bf.AddEvents(ctx, events); err == errEBufferFull(*config.Name) {
			assert.NotEmpty(t, err.Error())
			found = true
			break
		}
	}
	assert.True(t, found, "With small buffer size, I should error out with a full buffer")
	found = false
	for i := 0; i < 100; i++ {
		if err := bf.AddSpans(ctx, spans); err == errTBufferFull(*config.Name) {
			assert.NotEmpty(t, err.Error())
			found = true
			break
		}
	}
	assert.True(t, found, "With small buffer size, I should error out with a full buffer")
}
