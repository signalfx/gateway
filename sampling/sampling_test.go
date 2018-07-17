package sampling

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
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
				if err := sample.AddSpans(context.Background(), []*trace.Span{{TraceID: traceIDLow + traceIDHigh}}, next); err != nil {
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
			So(json.Unmarshal([]byte(fmt.Sprintf(`{"BaseRate": 0.07, "FalsePositiveRate": 0.0002, "Capacity": 10000, "CyclePeriod":"1s", "Adapt":true, "BackupLocation":"%s", "MaxSPM":1000000}`, dir)), &obj), ShouldBeNil)
			So(*obj.BaseRate, ShouldEqual, 0.07)
			So(*obj.FalsePositiveRate, ShouldEqual, 0.0002)
			So(*obj.Capacity, ShouldEqual, 10000)
			So(*obj.CyclePeriod, ShouldEqual, "1s")
			So(*obj.Adapt, ShouldEqual, true)
			sample, err := New(&obj, log.DefaultLogger)
			So(err, ShouldBeNil)
			So(sample.memory, ShouldEqual, 1*time.Second)
			runtime.Gosched()
			So(f(sample, 100000), ShouldBeNil)
			So(len(sample.Datapoints()), ShouldEqual, 3)
			sample.Close()
		})
		Convey("test throttling", func() {
			var obj SampleObj
			So(json.Unmarshal([]byte(fmt.Sprintf(`{"BaseRate": 0.07, "FalsePositiveRate": 0.0002, "Capacity": 10000, "CyclePeriod":"1s", "Adapt":true, "BackupLocation":"%s", "MaxSPM":100}`, dir)), &obj), ShouldBeNil)
			sample, err := New(&obj, log.DefaultLogger)
			for x := 0; x < 100000; x++ {
				traceIDLow := fmt.Sprintf("%08x", x)
				traceIDHigh := fmt.Sprintf("%08x", x)
				if err = sample.AddSpans(context.Background(), []*trace.Span{{TraceID: traceIDLow + traceIDHigh}}, next); err != nil {
					break
				}
			}
			So(err, ShouldNotBeNil)
			runtime.Gosched()
			So(atomic.LoadInt64(&sample.stats.droppedSpm), ShouldBeGreaterThan, 0)
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
			_, err := New(&obj, log.DefaultLogger)
			So(err, ShouldNotBeNil)
		})
		Reset(func() {
			os.RemoveAll(dir)
		})
	})
}

func TestSamples(t *testing.T) {
	dir, err := ioutil.TempDir("", "testing")
	tests := []struct {
		name, id string
		answer   bool
	}{
		{"bad length", "ZZZ", false},
		{"bad contents", "ZZZZZZZZZZZZZZZZ", false},
	}
	Convey("test samples", t, func() {
		So(err, ShouldBeNil)
		sample, err := New(&SampleObj{BaseRate: pointer.Float64(0.5), BackupLocation: &dir}, log.DefaultLogger)
		So(err, ShouldBeNil)
		Convey("test that once sampled or not sampled, they stay that way", func() {
			var sampled, notsampled string
			for x := 0; x < 100; x++ {
				traceIDLow := fmt.Sprintf("%016x", x)
				traceIDHigh := fmt.Sprintf("%016x", x)
				id := traceIDLow + traceIDHigh
				if sample.sample(id) {
					sampled = id
				} else {
					notsampled = id
				}
				if sampled != "" && notsampled != "" {
					break
				}
			}

			for x := 0; x < 100; x++ {
				So(sample.sample(sampled), ShouldBeTrue)
				So(sample.sample(notsampled), ShouldBeFalse)
			}

			Convey("test error stuff", func() {
				for _, test := range tests {
					Convey(test.name, func() {
						So(sample.sample(test.id), ShouldEqual, test.answer)
					})
				}
			})
		})
	})
	os.RemoveAll(dir)
}
