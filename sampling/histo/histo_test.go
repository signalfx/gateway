package histo

import (
	"fmt"
	"github.com/signalfx/go-metrics"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/golib/timekeeper/timekeepertest"
	"github.com/signalfx/metricproxy/sampling/histo/encoding"
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

func getSi() *encoding.SpanIdentity {
	return &encoding.SpanIdentity{
		Service:   "service",
		Operation: "operation",
	}
}

func Test(t *testing.T) {
	Convey("test existence", t, func() {
		dir, err := ioutil.TempDir("", "testing")
		So(err, ShouldBeNil)
		histo := New(path.Join(dir, "histo"), log.DefaultLogger, time.Millisecond*100, []float64{0.50})
		histo.Update(getSi(), 4.0)
		getMetric(histo, "proxy.tracing.totalUnique", 1)
		So(histo, ShouldNotBeNil)
		err = histo.Close()
		So(err, ShouldBeNil)
		// Do it again so it reads from the file
		histo = New(path.Join(dir, "histo"), log.DefaultLogger, time.Millisecond*100, []float64{0.50})
		getMetric(histo, "proxy.tracing.totalUnique", 1)
		So(histo, ShouldNotBeNil)
		err = histo.Close()
		So(err, ShouldBeNil)
		os.RemoveAll(dir)
	})
	Convey("test stuff", t, func() {
		tk := timekeepertest.NewStubClock(time.Now())
		dir, err := ioutil.TempDir("", "testing")
		So(err, ShouldBeNil)
		histo := newBag(path.Join(dir, "histo"), log.DefaultLogger, tk, time.Millisecond*10, time.Millisecond, 1000, DefaultMetricsReservoirSize, DefaultMetricsAlphaFactor, []float64{0.50})
		So(histo, ShouldNotBeNil)
		getMetric(histo, "proxy.tracing.totalSamples", 0)
		histo.Update(getSi(), 4.0)
		getMetric(histo, "proxy.tracing.totalSamples", 1)
		Println("incr time by 2 milliseconds, enough time for clean to run but not long enough for expire anything")
		tk.Incr(time.Millisecond * 2)
		getMetric(histo, "proxy.tracing.totalUnique", 1)

		Println("incr time by a enough time for clean to run and expire things")
		tk.Incr(time.Second)
		getMetric(histo, "proxy.tracing.totalUnique", 0)
		Reset(func() {
			histo.Close()
			os.RemoveAll(dir)
		})
	})
}

func TestBad(t *testing.T) {
	Convey("test bad stuff", t, func() {
		histo := &SpanHistoBag{
			digests:       make(map[encoding.SpanIdentity]metrics.Histogram),
			last:          make(map[encoding.SpanIdentity]time.Time),
			ch:            make(chan *payload, 1),
			done:          make(chan struct{}),
			dpsCh:         make(chan chan []*datapoint.Datapoint, 10),
			expiry:        time.Second,
			interval:      time.Second,
			reservoirSize: DefaultMetricsReservoirSize,
			alphaFactor:   DefaultMetricsAlphaFactor,
			quantiles:     []float64{0.50},
			tk:            timekeeper.RealTime{},
			logger:        log.DefaultLogger,
		}
		So(atomic.LoadInt64(&histo.stats.dropsBufferFull), ShouldEqual, 0)
		histo.Update(getSi(), 4)
		histo.Update(getSi(), 4)
		So(atomic.LoadInt64(&histo.stats.dropsBufferFull), ShouldEqual, 1)
		histo.drainFinal()
	})
}

func getMetric(histo *SpanHistoBag, metric string, value int) {
	var last datapoint.Value
	for i := 0; ; i++ {
		runtime.Gosched()
		if i > 1000000 {
			panic(fmt.Sprintf("unable to find metric %s with value %d; found Value %s", metric, value, last))
		}
		dps := histo.Datapoints()
		dp := dptest.ExactlyOne(dps, metric)
		if dp.Value.String() == strconv.Itoa(value) {
			break
		}
		last = dp.Value
	}
}
