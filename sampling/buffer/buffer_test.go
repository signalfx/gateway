package buffer

import (
	"context"
	"fmt"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/golib/timekeeper/timekeepertest"
	"github.com/signalfx/golib/trace"
	"github.com/signalfx/metricproxy/sampling/buffer/encoding"
	. "github.com/smartystreets/goconvey/convey"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func Test(t *testing.T) {
	sendTo := dptest.NewBasicSink()
	sendTo.Resize(10000)
	Convey("test buff", t, func() {
		dir, err := ioutil.TempDir("", "testing")
		So(err, ShouldBeNil)
		buf := New(path.Join(dir, "buffer"), time.Minute*10, sendTo, log.DefaultLogger)
		n := 10
		s := 55
		for i := 0; i < n; i++ {
			var spans []*trace.Span
			for j := 0; j < i+1; j++ {
				spans = append(spans, &trace.Span{TraceID: strconv.Itoa(j)})
			}
			buf.AddSpans(context.Background(), spans)
			runtime.Gosched()
		}
		getMetric(buf, "proxy.tracing.buffer.totalBufferedTraces", n)
		getMetric(buf, "proxy.tracing.buffer.totalBufferedSpans", s)
		err = buf.Close()
		So(err, ShouldBeNil)
		// do it again to read in
		buf = New(path.Join(dir, "buffer"), time.Minute*10, sendTo, log.DefaultLogger)
		getMetric(buf, "proxy.tracing.buffer.totalBufferedTraces", n)
		getMetric(buf, "proxy.tracing.buffer.totalBufferedSpans", s)
		err = buf.Close()
		So(err, ShouldBeNil)
		os.RemoveAll(dir)
	})
	Convey("test buff", t, func() {
		tk := timekeepertest.NewStubClock(time.Now())
		dir, err := ioutil.TempDir("", "testing")
		So(err, ShouldBeNil)
		buf := newBuff(path.Join(dir, "buffer"), tk, time.Minute*5, time.Minute, sendTo, log.DefaultLogger, 100, 100)
		sendTo.Resize(10)
		buf.AddSpans(context.Background(), []*trace.Span{
			{TraceID: "1"}, {TraceID: "1"},
			{TraceID: "2"}, {TraceID: "2"}, {TraceID: "2"},
		})
		getMetric(buf, "proxy.tracing.buffer.totalBufferedTraces", 2)
		buf.Release(pointer.String("2"))
		getMetric(buf, "proxy.tracing.buffer.totalBufferedTraces", 1)
		So(len(sendTo.TracesChan), ShouldEqual, 1)
		buf.AddSpans(context.Background(), []*trace.Span{
			{TraceID: "1"}, {TraceID: "1"},
			{TraceID: "2"}, {TraceID: "2"}, {TraceID: "2"},
		})
		for len(sendTo.TracesChan) != 4 {
			runtime.Gosched()
		}
		tk.Incr(time.Minute * 2) // long enough to run a clean, but nothing will expire
		getMetric(buf, "proxy.tracing.buffer.totalBufferedTraces", 1)
		tk.Incr(time.Minute * 4) // long enough for Traces to expire
		getMetric(buf, "proxy.tracing.buffer.totalBufferedTraces", 0)
		Reset(func() {
			err = buf.Close()
			So(err, ShouldBeNil)
			os.RemoveAll(dir)
		})
	})
}

func TestBad(t *testing.T) {
	Convey("test bad stuff", t, func() {
		sendTo := dptest.NewBasicSink()
		sendTo.Resize(10)
		ret := &BuffTrace{
			OnDisk: encoding.OnDisk{
				Traces:   make(map[string][]*trace.Span),
				Last:     make(map[string]time.Time),
				Remember: make(map[string]time.Time),
			},
			expiry:    time.Minute,
			interval:  time.Minute,
			sink:      sendTo,
			ch:        make(chan *trace.Span, 1),
			releaseCh: make(chan *samplePayload, 2),
			done:      make(chan struct{}),
			dps:       make(chan chan []*datapoint.Datapoint, 10),
			logger:    log.DefaultLogger,
			tk:        &timekeeper.RealTime{},
		}
		So(atomic.LoadInt64(&ret.stats.dropsBuffFull), ShouldEqual, 0)
		ret.AddSpans(context.Background(), []*trace.Span{
			{TraceID: "1"}, {TraceID: "1"},
		})
		So(atomic.LoadInt64(&ret.stats.dropsBuffFull), ShouldEqual, 1)
		resp := &samplePayload{
			traceID: pointer.String("1"),
			resp:    make(chan error, 1),
		}
		ret.releaseCh <- resp
		ret.drainFinal()
		So(len(resp.resp), ShouldEqual, 1)
	})
}

func getMetric(buff *BuffTrace, metric string, value int) {
	i := 0
	var last datapoint.Value
	for {
		if i > 0 && i%100000 == 0 {
			fmt.Println("looking for ", metric, "with", value, "Last", last)
			panic("oops")
		}
		runtime.Gosched()
		dps := buff.Datapoints()
		dp := dptest.ExactlyOne(dps, metric)
		if dp.Value.String() == strconv.Itoa(value) {
			break
		}
		last = dp.Value
		i++
	}
}
