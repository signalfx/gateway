package collectorhandler

import (
	"errors"
	"github.com/signalfx/golib/sfxclient"
	. "github.com/smartystreets/goconvey/convey"
	"net/http/httptest"
	"testing"
)

func Test(t *testing.T) {
	Convey("test internal metrics", t, func() {
		sched := sfxclient.NewScheduler()
		c := NewCollectorHandler(sched)
		req := httptest.NewRequest("GET", "/internal-metrics", nil)
		w := httptest.NewRecorder()
		c.DatapointsHandler(w, req)
		So(w.Body.String(), ShouldEqual, "[]")
	})
	Convey("test internal metrics", t, func() {
		sched := sfxclient.NewScheduler()
		c := NewCollectorHandler(sched)
		c.jsonMarshallerFunc = func(v interface{}) ([]byte, error) {
			return nil, errors.New("blarg")
		}
		req := httptest.NewRequest("GET", "/internal-metrics", nil)
		w := httptest.NewRecorder()
		c.DatapointsHandler(w, req)
		So(w.Body.String(), ShouldEqual, "blarg\n")
	})
}
