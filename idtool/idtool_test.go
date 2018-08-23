package idtool

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestGetIDAsString(t *testing.T) {
	Convey("test it", t, func() {
		So(GetIDAsString(1), ShouldEqual, "AAAAAAAAAAE")
		So(GetIDAsStringWithBuffer(0, make([]byte, 4)), ShouldEqual, "")
		So(GetStringAsID("AAAAAAAAAAA="), ShouldEqual, 0)
		So(GetStringAsID("AAAAAAAAAAA"), ShouldEqual, 0)
		So(GetStringAsID("ABCDEFGHIJKKK"), ShouldEqual, 0)
	})
}
