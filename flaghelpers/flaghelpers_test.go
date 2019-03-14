package flaghelpers

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestStringFlag(t *testing.T) {
	Convey("setup go max procs", t, func() {
		flag := NewStringFlag()
		So(flag, ShouldNotBeNil)
		So(flag.IsSet(), ShouldBeFalse)
		So(flag.String(), ShouldEqual, "")
		flag.Set("hello world")
		So(flag.IsSet(), ShouldBeTrue)
		So(flag.String(), ShouldEqual, "hello world")

	})
}
