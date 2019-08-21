package collectorhandler

import (
	"errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/sfxclient"
	. "github.com/smartystreets/goconvey/convey"
	"net/http/httptest"
	"testing"
)

func Test(t *testing.T) {
	Convey("test internal metrics", t, func() {
		sched := sfxclient.NewScheduler()
		c := NewCollectorHandler(log.Discard, sched)
		req := httptest.NewRequest("GET", "/internal-metrics", nil)
		w := httptest.NewRecorder()
		c.DatapointsHandler(w, req)
		So(w.Body.String(), ShouldEqual, "[]")
	})
	Convey("test internal metrics", t, func() {
		sched := sfxclient.NewScheduler()
		c := NewCollectorHandler(log.Discard, sched)
		c.jsonfunc = func(v interface{}) ([]byte, error) {
			return nil, errors.New("blarg")
		}
		req := httptest.NewRequest("GET", "/internal-metrics", nil)
		w := httptest.NewRecorder()
		c.DatapointsHandler(w, req)
		So(w.Body.String(), ShouldEqual, "blarg\n")
	})
}
