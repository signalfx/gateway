package signalfx

import (
	"github.com/signalfx/gateway/logkey"
	"github.com/signalfx/golib/sfxclient"
	. "github.com/smartystreets/goconvey/convey"
	"net/http/httptest"
	"testing"
)

func TestGetLogTokenFormat(t *testing.T) {
	Convey("test log token format", t, func() {
		req := httptest.NewRequest("get", "/index.html", nil)
		req.Header.Set(sfxclient.TokenHeaderName, "FIRST_HALF")
		ret := getTokenLogFormat(req)
		So(ret, ShouldResemble, []interface{}{logkey.SHA1, "dg7tp+xlWq6sb2Aj6lyRvaIYaXY=", logkey.Caller, "FIRST"})
		req.Header.Set(sfxclient.TokenHeaderName, "")
		ret = getTokenLogFormat(req)
		So(len(ret), ShouldResemble, 0)
	})
}
