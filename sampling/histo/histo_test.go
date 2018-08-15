package histo

import (
	"github.com/rcrowley/go-metrics"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/golib/timekeeper/timekeepertest"
	. "github.com/smartystreets/goconvey/convey"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func getSi() *SpanIdentity {
	return &SpanIdentity{
		Service:   "service",
		Operation: "operation",
	}
}

func Test(t *testing.T) {
	Convey("test existence", t, func() {
		histo := New(time.Millisecond*100, []float64{0.50})
		So(histo, ShouldNotBeNil)
		histo.Close()
	})
	Convey("test stuff", t, func() {
		tk := timekeepertest.NewStubClock(time.Now())
		histo := newBag(tk, time.Millisecond*10, time.Millisecond, 1000, DefaultMetricsReservoirSize, DefaultMetricsAlphaFactor, []float64{0.50})
		So(histo, ShouldNotBeNil)
		getMetric(histo, "proxy.tracing.totalSamples", 0)
		histo.Update(getSi(), 4.0)
		getMetric(histo, "proxy.tracing.totalSamples", 1)

		tk.Incr(time.Millisecond * 2) // enough time for clean to run but not long enough to expire anything
		getMetric(histo, "proxy.tracing.totalUnique", 1)

		tk.Incr(time.Millisecond * 20) // clean should have occured
		getMetric(histo, "proxy.tracing.totalUnique", 0)
		Reset(func() {
			histo.Close()
		})
	})
}

func TestBad(t *testing.T) {
	Convey("test bad stuff", t, func() {
		histo := &SpanHistoBag{
			digests:       make(map[SpanIdentity]metrics.Histogram),
			last:          make(map[SpanIdentity]time.Time),
			ch:            make(chan *payload, 1),
			done:          make(chan struct{}),
			dpsCh:         make(chan chan []*datapoint.Datapoint, 10),
			expiry:        time.Second,
			interval:      time.Second,
			reservoirSize: DefaultMetricsReservoirSize,
			alphaFactor:   DefaultMetricsAlphaFactor,
			quantiles:     []float64{0.50},
			tk:            timekeeper.RealTime{},
		}
		So(atomic.LoadInt64(&histo.stats.dropsBufferFull), ShouldEqual, 0)
		histo.Update(getSi(), 4)
		histo.Update(getSi(), 4)
		So(atomic.LoadInt64(&histo.stats.dropsBufferFull), ShouldEqual, 1)
		histo.drainFinal()
	})
}

func getMetric(histo *SpanHistoBag, metric string, value int) {
	for {
		runtime.Gosched()
		dps := histo.Datapoints()
		dp := dptest.ExactlyOne(dps, metric)
		if dp.Value.String() == strconv.Itoa(value) {
			break
		}
	}
}
