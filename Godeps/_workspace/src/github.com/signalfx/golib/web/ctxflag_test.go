package web

import (
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
	"net/http"
	"net/http/httptest"
	"testing"
)

type handleCall struct {
	ctx context.Context
	rw  http.ResponseWriter
	r   *http.Request
}

type handleStack []handleCall

func (h *handleStack) ServeHTTPC(ctx context.Context, rw http.ResponseWriter, r *http.Request) {
	*h = append(*h, handleCall{ctx, rw, r})
}

func TestHeaderCtxFlag(t *testing.T) {
	Convey("When setup,", t, func() {
		h := HeaderCtxFlag{}
		hs := handleStack([]handleCall{})
		ctx := context.Background()
		Convey("should not setup by default,", func() {
			req, err := http.NewRequest("", "", nil)
			So(err, ShouldBeNil)
			rw := httptest.NewRecorder()
			h.CreateMiddleware(&hs).ServeHTTPC(ctx, rw, req)
			So(len(hs), ShouldEqual, 1)
			So(h.HasFlag(hs[0].ctx), ShouldBeFalse)
		})
		Convey("With headername set,", func() {
			h.HeaderName = "X-Debug"
			Convey("And flag set,", func() {
				h.SetFlagStr("enabled")
				Convey("should fail when not set correctly,", func() {
					req, err := http.NewRequest("", "", nil)
					So(err, ShouldBeNil)
					req.Header.Add(h.HeaderName, "not-enabled")
					rw := httptest.NewRecorder()
					h.ServeHTTPC(ctx, rw, req, &hs)
					So(len(hs), ShouldEqual, 1)
					So(h.HasFlag(hs[0].ctx), ShouldBeFalse)
				})
				Convey("should check headers,", func() {
					req, err := http.NewRequest("", "", nil)
					So(err, ShouldBeNil)
					req.Header.Add(h.HeaderName, "enabled")
					rw := httptest.NewRecorder()
					h.ServeHTTPC(ctx, rw, req, &hs)
					So(len(hs), ShouldEqual, 1)
					So(h.HasFlag(hs[0].ctx), ShouldBeTrue)
				})
				Convey("should check query params,", func() {
					req, err := http.NewRequest("GET", "http://localhost?X-Debug=enabled", nil)
					So(err, ShouldBeNil)
					rw := httptest.NewRecorder()
					h.ServeHTTPC(ctx, rw, req, &hs)
					So(len(hs), ShouldEqual, 1)
					So(h.HasFlag(hs[0].ctx), ShouldBeTrue)
				})
			})

		})
	})

}
