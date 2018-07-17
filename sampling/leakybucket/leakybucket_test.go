package leakybucket

import (
	"github.com/signalfx/golib/timekeeper/timekeepertest"
	. "github.com/smartystreets/goconvey/convey"
	"math"
	"testing"
	"time"
)

func Test(t *testing.T) {
	Convey("test new", t, func() {
		tk := timekeepertest.NewStubClock(time.Now())
		lb := New(tk, 1000, time.Second)
		begin := lb.String()
		So(lb.CheckAction(100), ShouldBeNil)
		tk.Incr(1100 * time.Millisecond)
		So(lb.Fill(1100), ShouldNotBeNil)
		So(lb.ActionsThisDuration(), ShouldEqual, 1100)
		So(lb.CurrentActionLimit(), ShouldEqual, 1000)
		So(lb.GenerateThrottleResponse().String(), ShouldEqual, "ThrottlingResponse({Owner:TokenOwner({OrgId:ID({ID:0}) TeamId:ID({ID:0}) TeamName: NamedTokenId:ID({ID:0})}) Resource:<UNSET> LimitPerTimeUnit:1000 TimeUnitInNs:1000000000 CurrentUsed:1100 Spread:-1 TimeLeftInPeriod:1100000000 Timestamp:0 Sender:})")
		lb.Reset()
		So(lb.String(), ShouldEqual, begin)
		lb.ChangeActionLimit(500)
		So(lb.CheckAction(600), ShouldNotBeNil)
		lb.ChangeActionLimit(0)
		So(lb.CheckAction(1), ShouldNotBeNil)
		lb.ChangeActionLimit(math.MaxInt64)
		So(lb.CurrentActionLimit(), ShouldEqual, math.MaxInt64)
		So(lb.CheckAction(1), ShouldBeNil)
	})
}
