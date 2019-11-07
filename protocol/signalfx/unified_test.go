package signalfx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/datapoint/dpsink"
	"github.com/signalfx/golib/v3/datapoint/dptest"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/log"
	"github.com/signalfx/golib/v3/trace"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

type expect struct {
	count     int
	forwardTo Sink
}

func (e *expect) AddEvents(ctx context.Context, events []*event.Event) error {
	if len(events) != e.count {
		panic("NOPE")
	}
	if e.forwardTo != nil {
		events = append(events, nil)
		log.IfErr(log.Panic, e.forwardTo.AddEvents(ctx, events))
	}
	return nil
}

func (e *expect) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	if len(points) != e.count {
		panic("NOPE")
	}
	if e.forwardTo != nil {
		points = append(points, nil)
		log.IfErr(log.Panic, e.forwardTo.AddDatapoints(ctx, points))
	}
	return nil
}

func (e *expect) AddSpans(ctx context.Context, spans []*trace.Span) error {
	if len(spans) != e.count {
		panic("NOPE")
	}
	if e.forwardTo != nil {
		spans = append(spans, nil)
		log.IfErr(log.Panic, e.forwardTo.AddSpans(ctx, spans))
	}
	return nil
}

func (e *expect) next(sendTo Sink) Sink {
	return &expect{
		count:     e.count,
		forwardTo: sendTo,
	}
}

func TestFromChain(t *testing.T) {
	e2 := expect{count: 2}
	e1 := expect{count: 1}
	e0 := expect{count: 0}

	chain := FromChain(&e2, e0.next, e1.next)
	log.IfErr(log.Panic, chain.AddDatapoints(nil, []*datapoint.Datapoint{}))
	log.IfErr(log.Panic, chain.AddEvents(nil, []*event.Event{}))
}

func TestIncludingDimensions(t *testing.T) {
	Convey("With a basic sink", t, func() {
		end := dptest.NewBasicSink()
		end.Resize(1)
		addInto := IncludingDimensions(map[string]string{"name": "jack"}, end)
		ctx := context.Background()
		Convey("no dimensions should be identity function", func() {
			addInto = IncludingDimensions(nil, end)
			So(addInto, ShouldEqual, end)
		})

		Convey("appending dims should work for datapoints", func() {
			dp := dptest.DP()
			So(addInto.AddDatapoints(ctx, []*datapoint.Datapoint{dp}), ShouldBeNil)
			dpOut := end.Next()
			So(dpOut.Dimensions["name"], ShouldEqual, "jack")

			w := &WithDimensions{}
			So(len(w.appendDimensions(nil)), ShouldEqual, 0)
		})
		Convey("appending dims should work for events", func() {
			e := dptest.E()
			So(addInto.AddEvents(ctx, []*event.Event{e}), ShouldBeNil)
			eOut := end.NextEvent()
			So(eOut.Dimensions["name"], ShouldEqual, "jack")

			w := &WithDimensions{}
			So(len(w.appendDimensionsEvents(nil)), ShouldEqual, 0)
		})
		Convey("appending dims should be a pass through for traces", func() {
			So(addInto.AddSpans(ctx, []*trace.Span{{}}), ShouldBeNil)
		})
	})
}

type boolFlagCheck bool

func (b *boolFlagCheck) HasFlag(ctx context.Context) bool {
	return bool(*b)
}

func TestFilter(t *testing.T) {
	Convey("With item flagger", t, func() {
		flagCheck := boolFlagCheck(false)
		i := &dpsink.ItemFlagger{
			CtxFlagCheck:        &flagCheck,
			EventMetaName:       "my_events",
			MetricDimensionName: "sf_metric",
			Logger:              log.Discard,
		}
		dp1 := datapoint.New("mname", map[string]string{"org": "mine", "type": "prod"}, nil, datapoint.Gauge, time.Time{})
		dp2 := datapoint.New("mname2", map[string]string{"org": "another", "type": "prod"}, nil, datapoint.Gauge, time.Time{})
		ev1 := event.New("mname", event.USERDEFINED, map[string]string{"org": "mine", "type": "prod"}, time.Time{})
		ev2 := event.New("mname2", event.USERDEFINED, map[string]string{"org": "another", "type": "prod"}, time.Time{})
		chain := FromChain(dpsink.Discard, NextWrap(UnifyNextSinkWrap(i)))
		ctx := context.Background()
		So(len(i.Datapoints()), ShouldEqual, 4)
		Convey("should not flag by default", func() {
			So(chain.AddDatapoints(ctx, []*datapoint.Datapoint{dp1, dp2}), ShouldBeNil)
			So(i.HasDatapointFlag(dp1), ShouldBeFalse)
			So(i.HasDatapointFlag(dp2), ShouldBeFalse)

			So(chain.AddEvents(ctx, []*event.Event{ev1, ev2}), ShouldBeNil)
			So(i.HasEventFlag(ev1), ShouldBeFalse)
			So(i.HasEventFlag(ev2), ShouldBeFalse)
			So(i.AddSpans(ctx, []*trace.Span{}, dpsink.Discard), ShouldBeNil)
		})
		Convey("should flag if context is flagged", func() {
			flagCheck = boolFlagCheck(true)
			So(chain.AddDatapoints(ctx, []*datapoint.Datapoint{dp1, dp2}), ShouldBeNil)
			So(i.HasDatapointFlag(dp1), ShouldBeTrue)
			So(i.HasDatapointFlag(dp2), ShouldBeTrue)

			So(chain.AddEvents(ctx, []*event.Event{ev1, ev2}), ShouldBeNil)
			So(i.HasEventFlag(ev1), ShouldBeTrue)
			So(i.HasEventFlag(ev2), ShouldBeTrue)
		})
		Convey("should flag if dimensions are flagged", func() {
			i.SetDimensions(map[string]string{"org": "mine"})
			So(i.Var().String(), ShouldContainSubstring, "mine")
			So(chain.AddDatapoints(ctx, []*datapoint.Datapoint{dp1, dp2}), ShouldBeNil)
			So(i.HasDatapointFlag(dp1), ShouldBeTrue)
			So(i.HasDatapointFlag(dp2), ShouldBeFalse)

			So(chain.AddEvents(ctx, []*event.Event{ev1, ev2}), ShouldBeNil)
			So(i.HasEventFlag(ev1), ShouldBeTrue)
			So(i.HasEventFlag(ev2), ShouldBeFalse)
		})

		Convey("should flag if metric is flagged", func() {
			i.SetDimensions(map[string]string{"sf_metric": "mname2"})
			So(chain.AddDatapoints(ctx, []*datapoint.Datapoint{dp1, dp2}), ShouldBeNil)
			So(i.HasDatapointFlag(dp1), ShouldBeFalse)
			So(i.HasDatapointFlag(dp2), ShouldBeTrue)
		})

		Convey("Invalid POST should return an error", func() {
			req, err := http.NewRequest("POST", "", strings.NewReader(`_INVALID_JSON`))
			So(err, ShouldBeNil)
			rw := httptest.NewRecorder()
			i.ServeHTTP(rw, req)
			So(rw.Code, ShouldEqual, http.StatusBadRequest)
		})

		Convey("POST should change dimensions", func() {
			req, err := http.NewRequest("POST", "", strings.NewReader(`{"name":"jack"}`))
			So(err, ShouldBeNil)
			rw := httptest.NewRecorder()
			i.ServeHTTP(rw, req)
			So(rw.Code, ShouldEqual, http.StatusOK)
			So(i.GetDimensions(), ShouldResemble, map[string]string{"name": "jack"})
			Convey("and GET should return them", func() {
				req, err := http.NewRequest("GET", "", nil)
				So(err, ShouldBeNil)
				rw := httptest.NewRecorder()
				i.ServeHTTP(rw, req)
				So(rw.Code, ShouldEqual, http.StatusOK)
				So(rw.Body.String(), ShouldEqual, `{"name":"jack"}`+"\n")
			})
		})
		Convey("PATCH should 404", func() {
			req, err := http.NewRequest("PATCH", "", strings.NewReader(`{"name":"jack"}`))
			So(err, ShouldBeNil)
			rw := httptest.NewRecorder()
			i.ServeHTTP(rw, req)
			So(rw.Code, ShouldEqual, http.StatusNotFound)
		})
	})
}

const numTests = 10

func TestCounterSink(t *testing.T) {
	dps := []*datapoint.Datapoint{
		{},
		{},
	}
	ctx := context.Background()
	bs := dptest.NewBasicSink()
	count := &dpsink.Counter{
		Logger: log.Discard,
	}
	middleSink := NextWrap(UnifyNextSinkWrap(count))(bs)
	go func() {
		// Allow time for us to get in the middle of a call
		time.Sleep(time.Millisecond)
		assert.Equal(t, int64(1), atomic.LoadInt64(&count.CallsInFlight), "After a sleep, should be in flight")
		datas := <-bs.PointsChan
		assert.Equal(t, 2, len(datas), "Original datas should be sent")
	}()
	log.IfErr(log.Panic, middleSink.AddDatapoints(ctx, dps))
	assert.Equal(t, int64(0), atomic.LoadInt64(&count.CallsInFlight), "Call is finished")
	assert.Equal(t, int64(0), atomic.LoadInt64(&count.TotalProcessErrors), "No errors so far (see above)")
	assert.Equal(t, numTests, len(count.Datapoints()), "Just checking stats len()")

	bs.RetError(errors.New("nope"))
	if err := middleSink.AddDatapoints(ctx, dps); err == nil {
		t.Fatal("Expected an error!")
	}
	assert.Equal(t, int64(1), atomic.LoadInt64(&count.TotalProcessErrors), "Error should be sent through")
}

func TestCounterSinkEvent(t *testing.T) {
	es := []*event.Event{
		{},
		{},
	}
	ctx := context.Background()
	bs := dptest.NewBasicSink()
	count := &dpsink.Counter{}
	middleSink := NextWrap(UnifyNextSinkWrap(count))(bs)
	go func() {
		// Allow time for us to get in the middle of a call
		time.Sleep(time.Millisecond)
		assert.Equal(t, int64(1), atomic.LoadInt64(&count.CallsInFlight), "After a sleep, should be in flight")
		datas := <-bs.EventsChan
		assert.Equal(t, 2, len(datas), "Original datas should be sent")
	}()
	log.IfErr(log.Panic, middleSink.AddEvents(ctx, es))
	assert.Equal(t, int64(0), atomic.LoadInt64(&count.CallsInFlight), "Call is finished")
	assert.Equal(t, int64(0), atomic.LoadInt64(&count.TotalProcessErrors), "No errors so far (see above)")
	assert.Equal(t, numTests, len(count.Datapoints()), "Just checking stats len()")

	bs.RetError(errors.New("nope"))
	if err := middleSink.AddEvents(ctx, es); err == nil {
		t.Fatal("Expected an error!")
	}
	assert.Equal(t, int64(1), atomic.LoadInt64(&count.TotalProcessErrors), "Error should be sent through")
}

func TestCounterSinkTrace(t *testing.T) {
	es := []*trace.Span{
		{},
		{},
	}
	dcount := &dpsink.Counter{}
	ctx := context.Background()
	sink := dptest.NewBasicSink()
	counter := UnifyNextSinkWrap(dcount)
	finalSink := FromChain(sink, NextWrap(counter))

	go func() {
		// Allow time for us to get in the middle of a call
		time.Sleep(time.Millisecond)
		assert.Equal(t, int64(1), atomic.LoadInt64(&dcount.CallsInFlight), "After a sleep, should be in flight")
		spans := <-sink.TracesChan
		assert.Equal(t, 2, len(spans), "Original spans should be sent")
	}()
	log.IfErr(log.Panic, finalSink.AddSpans(ctx, es))
	assert.Equal(t, int64(0), atomic.LoadInt64(&dcount.CallsInFlight), "Call is finished")
	assert.Equal(t, int64(0), atomic.LoadInt64(&dcount.TotalProcessErrors), "No errors so far (see above)")
	assert.Equal(t, numTests, len(dcount.Datapoints()), "Just checking stats len()")

	for _, v := range dcount.Datapoints() {
		if v.Metric == "total_spans" {
			fmt.Println(v)
		}
	}
	for _, v := range dcount.Datapoints() {
		if v.Metric == "total_spans" {
			fmt.Println(v)
		}
	}

	sink.RetError(errors.New("nope"))
	if err := finalSink.AddSpans(ctx, es); err == nil {
		t.Fatal("Expected an error!")
	}
	assert.Equal(t, int64(1), atomic.LoadInt64(&dcount.TotalProcessErrors), "Error should be sent through")
}
