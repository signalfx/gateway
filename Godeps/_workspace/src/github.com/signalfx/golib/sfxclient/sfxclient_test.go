package sfxclient

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/timekeeper/timekeepertest"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
)

type testSink struct {
	retErr         error
	lastDatapoints chan []*datapoint.Datapoint
}

func (t *testSink) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) (err error) {
	t.lastDatapoints <- points
	return t.retErr
}

func TestNewScheduler(t *testing.T) {
	Convey("Default error handler should not panic", t, func() {
		So(func() { DefaultErrorHandler(errors.New("test")) }, ShouldNotPanic)
		So(func() { DefaultErrorHandler(nil) }, ShouldPanic)
	})

	Convey("with a testing scheduler", t, func() {
		s := NewScheduler()

		tk := timekeepertest.NewStubClock(time.Now())
		s.Timer = tk

		sink := &testSink{
			lastDatapoints: make(chan []*datapoint.Datapoint, 1),
		}
		s.Sink = sink
		s.ReportingDelay(time.Second)

		var handledErrors []error
		var handleErrRet error
		s.ErrorHandler = func(e error) error {
			handledErrors = append(handledErrors, e)
			return errors.Wrap(handleErrRet, e)
		}

		ctx := context.Background()

		Convey("removing a callback that doesn't exist should work", func() {
			s.RemoveCallback(nil)
			s.RemoveGroupedCallback("_", nil)
		})

		Convey("a single report should work", func() {
			So(s.ReportOnce(ctx), ShouldBeNil)
			firstPoints := <-sink.lastDatapoints
			So(len(firstPoints), ShouldEqual, 0)
			So(len(sink.lastDatapoints), ShouldEqual, 0)
		})
		Convey("with a single callback", func() {
			s.AddCallback(GoMetricsSource)
			Convey("default dimensions should set", func() {
				s.DefaultDimensions(map[string]string{"host": "bob"})
				s.GroupedDefaultDimensions("_", map[string]string{"host": "bob2"})
				So(s.ReportOnce(ctx), ShouldBeNil)
				firstPoints := <-sink.lastDatapoints
				So(firstPoints[0].Dimensions["host"], ShouldEqual, "bob")
				Convey("and var should return previous points", func() {
					So(s.Var().String(), ShouldContainSubstring, "bob")
				})
			})
			Convey("and made to error out", func() {
				sink.retErr = errors.New("nope bad done")
				handleErrRet = errors.New("handle error is bad")
				Convey("scheduled should end", func() {
					scheduleOver := int64(0)
					go func() {
						for atomic.LoadInt64(&scheduleOver) == 0 {
							tk.Incr(time.Duration(s.ReportingDelayNs))
							runtime.Gosched()
						}
					}()
					err := s.Schedule(ctx)
					atomic.StoreInt64(&scheduleOver, 1)
					So(err.Error(), ShouldEqual, "nope bad done")
					So(errors.Details(err), ShouldContainSubstring, "handle error is bad")
				})
			})
			Convey("and scheduled", func() {
				scheduledContext, cancelFunc := context.WithCancel(ctx)
				scheduleRetErr := make(chan error)
				go func() {
					scheduleRetErr <- s.Schedule(scheduledContext)
				}()
				Convey("should collect when time advances", func() {
					for len(sink.lastDatapoints) == 0 {
						tk.Incr(time.Duration(s.ReportingDelayNs))
						runtime.Gosched()
					}
					firstPoints := <-sink.lastDatapoints
					So(len(firstPoints), ShouldEqual, 30)
					So(len(sink.lastDatapoints), ShouldEqual, 0)
					Convey("and should skip an interval if we sleep too long", func() {
						// Should eventually end
						for atomic.LoadInt64(&s.stats.resetIntervalCounts) == 0 {
							// Keep skipping time and draining outstanding points until we skip an interval
							tk.Incr(time.Duration(s.ReportingDelayNs) * 3)
							for len(sink.lastDatapoints) > 0 {
								<-sink.lastDatapoints
							}
							runtime.Gosched()
						}
					})
				})
				Reset(func() {
					cancelFunc()
					<-scheduleRetErr
				})
			})
			Reset(func() {
				s.RemoveCallback(GoMetricsSource)
			})
		})

		Reset(func() {
			close(sink.lastDatapoints)
		})
	})
}
