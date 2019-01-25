package sampling

import (
	"testing"

	"context"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/trace"
	. "github.com/smartystreets/goconvey/convey"
	"net/http"
)

type end struct {
}

func (e *end) AddSpans(ctx context.Context, spans []*trace.Span) error {
	return nil
}

func (e *end) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	return nil
}

func (e *end) AddEvents(ctx context.Context, events []*event.Event) error {
	return nil
}
func Test(t *testing.T) {
	Convey("test smart sampler stub", t, func() {
		obj := new(SmartSampleConfig)
		n, err := New(obj, log.Discard, nil)
		So(n, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(n.AddSpans(context.Background(), []*trace.Span{}, &end{}), ShouldBeNil)
		So(n.StartupFinished(), ShouldBeNil)
		So(n.Close(), ShouldBeNil)
		So(len(n.Datapoints()), ShouldEqual, 0)
		So(n.DebugEndpoints(), ShouldResemble, map[string]http.Handler{})
	})
}
