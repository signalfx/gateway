package sampling

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/timekeeper/timekeepertest"
	"github.com/signalfx/golib/trace"
	. "github.com/smartystreets/goconvey/convey"
	"io/ioutil"
	"math"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type end struct {
	count int64
}

func (t *end) AddSpans(ctx context.Context, traces []*trace.Span) error {
	atomic.AddInt64(&t.count, 1)
	return nil
}

func Test(t *testing.T) {
	Convey("test sampling", t, func() {
		dir, err := ioutil.TempDir("", "testing")
		So(err, ShouldBeNil)
		next := &end{}
		f := func(sample *SampleForwarder, n int) error {
			for x := 0; x < n; x++ {
				traceIDLow := fmt.Sprintf("%08x", x)
				traceIDHigh := fmt.Sprintf("%08x", x)
				var err error
				if x == 0 {
					err = sample.AddSpans(context.Background(), []*trace.Span{
						{TraceID: traceIDLow + traceIDHigh,
							Duration: pointer.Float64(float64(x) / 100),
						},
					}, next)
				} else {
					err = sample.AddSpans(context.Background(), []*trace.Span{
						{
							TraceID:       traceIDLow + traceIDHigh,
							Name:          pointer.String("component"),
							LocalEndpoint: &trace.Endpoint{ServiceName: pointer.String("service")},
							Duration:      pointer.Float64(float64(x) / 100)},
					}, next)
				}
				if err != nil {
					return err
				}
			}
			s := atomic.LoadInt64(&sample.stats.sampled)
			ns := atomic.LoadInt64(&sample.stats.notSampled)
			floatHitRate := float64(s) / float64(ns)
			diff := math.Abs(floatHitRate - sample.baseRate)
			Println(s, ns, floatHitRate, diff)
			So(diff, ShouldBeLessThan, 0.01)
			return nil
		}
		Convey("test non empty json", func() {
			var obj SampleObj
			tk := timekeepertest.NewStubClock(time.Now())
			So(json.Unmarshal([]byte(fmt.Sprintf(`{"BaseRate": 0.07, "FalsePositiveRate": 0.0002, "Capacity": 10000, "CyclePeriod":"1s", "Adapt":true, "BackupLocation":"%s"}`, dir)), &obj), ShouldBeNil)
			So(*obj.BaseRate, ShouldEqual, 0.07)
			So(*obj.FalsePositiveRate, ShouldEqual, 0.0002)
			So(*obj.Capacity, ShouldEqual, 10000)
			So(*obj.CyclePeriod, ShouldEqual, "1s")
			So(*obj.Adapt, ShouldEqual, true)
			sample, err := newSampler(tk, &obj, log.DefaultLogger, next)
			So(err, ShouldBeNil)
			So(sample.memory, ShouldEqual, 1*time.Second)
			tk.Incr(time.Second)
			runtime.Gosched()
			So(f(sample, 10000), ShouldBeNil)
			So(len(sample.Datapoints()), ShouldEqual, 26)
			tk.Incr(time.Second * 3)
			runtime.Gosched()
			So(f(sample, 10), ShouldBeNil) // needs at least one span to rotate
			So(sample.overSample(&trace.Span{TraceID: "blarg"}, map[string]bool{"blarg": true}), ShouldBeTrue)
			sample.Close()
		})
		Convey("test sampling rotation", func() {

		})
		Convey("test nil", func() {
			var sample *SampleForwarder
			So(len(sample.Datapoints()), ShouldEqual, 0)
			So(sample.AddSpans(context.Background(), []*trace.Span{{}}, next), ShouldBeNil)
		})
		Convey("test bad memory", func() {
			obj := SampleObj{
				CyclePeriod: pointer.String("foo"),
			}
			_, err := New(&obj, log.DefaultLogger, next)
			So(err, ShouldNotBeNil)
		})
		Reset(func() {
			os.RemoveAll(dir)
		})
	})
}

func TestSamples(t *testing.T) {
	dir, err := ioutil.TempDir("", "testing")
	next := &end{}
	tests := []struct {
		name, id string
		answer   bool
	}{
		{"bad length", "ZZZ", false},
		{"bad contents", "ZZZZZZZZZZZZZZZZ", false},
	}
	Convey("test samples", t, func() {
		So(err, ShouldBeNil)
		sample, err := New(&SampleObj{BaseRate: pointer.Float64(0.5), BackupLocation: &dir}, log.DefaultLogger, next)
		So(err, ShouldBeNil)
		Convey("test that once sampled or not sampled, they stay that way", func() {
			var sampled, notsampled string
			for x := 0; x < 100; x++ {
				traceIDLow := fmt.Sprintf("%016x", x)
				traceIDHigh := fmt.Sprintf("%016x", x)
				id := traceIDLow + traceIDHigh
				if sample.baseSample(id) {
					sampled = id
				} else {
					notsampled = id
				}
				if sampled != "" && notsampled != "" {
					break
				}
			}

			for x := 0; x < 100; x++ {
				So(sample.baseSample(sampled), ShouldBeTrue)
				So(sample.baseSample(notsampled), ShouldBeFalse)
			}

			Convey("test error stuff", func() {
				for _, test := range tests {
					Convey(test.name, func() {
						So(sample.baseSample(test.id), ShouldEqual, test.answer)
					})
				}
			})
		})
	})
	os.RemoveAll(dir)
}
