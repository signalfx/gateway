package sampling

import (
	"testing"

	"context"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/trace"
	. "github.com/smartystreets/goconvey/convey"
)

func Test(t *testing.T) {
	Convey("test smart sampler stub", t, func() {
		obj := new(SmartSampleConfig)
		n, err := New(obj, log.Discard, nil)
		So(n, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(n.AddSpans(context.Background(), []*trace.Span{}, nil), ShouldBeNil)
		So(n.StartupFinished(), ShouldBeNil)
		So(n.Close(), ShouldBeNil)
		So(len(n.Datapoints()), ShouldEqual, 0)
	})
}
