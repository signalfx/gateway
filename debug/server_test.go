package debug

import (
	"fmt"
	"github.com/signalfx/golib/nettest"
	. "github.com/smartystreets/goconvey/convey"
	"net/http"
	"testing"
)

func TestDebugServer(t *testing.T) {
	Convey("debug server should be setupable", t, func() {
		explorable := "testing"
		ser, err := New("127.0.0.1:0", explorable, nil)
		So(err, ShouldBeNil)
		listenPort := nettest.TCPPort(ser)
		serverURL := fmt.Sprintf("http://127.0.0.1:%d", listenPort)
		Convey("and find commandline", func() {
			resp, err := http.Get(serverURL + "/debug/pprof/cmdline")
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
		})

		Reset(func() {
			So(ser.Close(), ShouldBeNil)
		})
	})
	Convey("debug server should fail on bad listen addrs", t, func() {
		_, err := New("127.0.0.1:999999", nil, nil)
		So(err, ShouldNotBeNil)
	})
}
