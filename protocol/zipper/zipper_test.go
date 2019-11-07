package zipper

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/signalfx/golib/v3/errors"
	. "github.com/smartystreets/goconvey/convey"
)

type foo struct {
}

func (f *foo) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(req.Body)
	if err == nil {
		if "OK" == string(buf.Bytes()) {
			rw.WriteHeader(http.StatusOK)
		}
	}
	fmt.Println(err)
	rw.WriteHeader(http.StatusBadRequest)
	return
}

func Test(t *testing.T) {
	Convey("Test zipper", t, func() {
		zippers := NewZipper()
		badZippers := newZipper(func(r io.Reader) (*gzip.Reader, error) {
			return new(gzip.Reader), errors.New("nope")
		})
		f := new(foo)
		zipped := new(bytes.Buffer)
		w := gzip.NewWriter(zipped)
		_, err := w.Write([]byte("OK"))
		So(err, ShouldBeNil)
		So(w.Close(), ShouldBeNil)
		tests := []struct {
			zipper  *ReadZipper
			name    string
			data    []byte
			status  int
			headers map[string]string
		}{
			{zippers, "test non gzipped", []byte("OK"), http.StatusOK, map[string]string{}},
			{zippers, "test gzipped", zipped.Bytes(), http.StatusOK, map[string]string{"Content-Encoding": "gzip"}},
			{zippers, "test gzipped but bad", zipped.Bytes()[:5], http.StatusBadRequest, map[string]string{"Content-Encoding": "gzip"}},
			{badZippers, "test gzipped failure", zipped.Bytes(), http.StatusBadRequest, map[string]string{"Content-Encoding": "gzip"}},
		}
		for _, test := range tests {
			Convey(test.name, func() {
				req, err := http.NewRequest("GET", "/health-check", bytes.NewBuffer(test.data))
				for k, v := range test.headers {
					req.Header.Set(k, v)
				}
				So(err, ShouldBeNil)
				rr := httptest.NewRecorder()
				g := test.zipper.GzipHandler(f)
				g.ServeHTTP(rr, req)
				So(rr.Code, ShouldEqual, test.status)

			})
		}
		Convey("check datapoints", func() {
			So(len(zippers.Datapoints()), ShouldEqual, 4)
		})
	})
}
