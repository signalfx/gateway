package ddagent

import (
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/dp/dptest"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
)

type OkHandler struct {
	reqCount int64
}

func (o *OkHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	rw.WriteHeader(http.StatusOK)
	atomic.AddInt64(&o.reqCount, 1)
}

func metricFromDatapoints(dps []*datapoint.Datapoint, mname string) *datapoint.Datapoint {
	for _, dp := range dps {
		if dp.Metric == mname {
			return dp
		}
	}
	return nil
}

func TestNewListenerErrors(t *testing.T) {
	Convey("Invalid listen address should error", t, func() {
		conf := config.ListenFrom{
			ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("1:2:3:2:45"),
		}
		_, err := ListenerLoader(nil, nil, &conf)
		So(err, ShouldNotBeNil)
	})
	Convey("Invalid url.Parse should error", t, func() {
		err := errors.New("nope")
		f := func(string) (*url.URL, error) {
			return nil, err
		}
		_, err2 := NewListener(nil, nil, "127.0.0.1:123", time.Second, time.Second, "", "127.0.0.1:0", f)
		So(err2, ShouldEqual, err)
	})
}

type emptyForwarder struct{}

func (e emptyForwarder) HandleForwardedResponse(reforwardError error, bodyBuffer io.Reader, rw http.ResponseWriter, req *http.Request) {
}

type errReader struct{}

func (e *errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("nope")
}

func TestDdagentForwarder(t *testing.T) {
	Convey("Forwarder copy errors should track", t, func() {
		handler := emptyForwarder{}
		f := Forwarder{
			Handler: &handler,
		}
		req, _ := http.NewRequest("POST", "/", &errReader{})
		rw := httptest.NewRecorder()
		f.ServeHTTPC(nil, rw, req)
		So(f.errCopyingBuffer, ShouldEqual, 1)
	})
}

func TestResendForwarderError(t *testing.T) {
	Convey("Forwarder to invalid host should track", t, func() {
		rw := httptest.NewRecorder()
		r := ResendForwarder{}
		r.parsedResendHost.Host = "www.example.com:2"
		req, _ := http.NewRequest("POST", "/post", nil)
		r.HandleForwardedResponse(nil, &bytes.Buffer{}, rw, req)
		So(rw.Code, ShouldEqual, http.StatusInternalServerError)
	})

	Convey("Forwarder to http.NewRequest failure should error", t, func() {
		r := ResendForwarder{}
		r.parsedResendHost.Host = "www.example.com:2"
		req, _ := http.NewRequest("POST", "/post", nil)
		errNope := errors.New("nope")
		f := func(method, urlStr string, body io.Reader) (*http.Request, error) {
			return nil, errNope
		}
		_, err := r.forwardReq(nil, req, f)
		So(err.error, ShouldEqual, errNope)
	})
}

func TestDdagentListenerSetup(t *testing.T) {
	testWithReforward := func(reforwardToAHost bool) func() {
		return func() {
			handler := OkHandler{}
			server := httptest.NewServer(&handler)
			ctx := context.Background()
			sink := dptest.NewBasicSink()
			conf := config.ListenFrom{
				ListenAddr:    workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0"),
				ReforwardHost: workarounds.GolangDoesnotAllowPointerToStringLiteral(server.URL),
			}
			if !reforwardToAHost {
				conf.ReforwardHost = workarounds.GolangDoesnotAllowPointerToStringLiteral("")
			}
			client := http.Client{}

			l, err := ListenerLoader(ctx, sink, &conf)
			So(err, ShouldBeNil)

			listenedPort := l.listener.Addr().(*net.TCPAddr).Port

			testAPost := func(toPost string, extraHeaders map[string]string, expectDatapoints bool) {
				body := bytes.NewBufferString(toPost)
				req, err := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%d/test", listenedPort), body)
				for k, v := range extraHeaders {
					req.Header.Add(k, v)
				}
				So(err, ShouldBeNil)
				var seenDps []*datapoint.Datapoint
				seenDatapoints := make(chan struct{})
				go func() {
					defer close(seenDatapoints)
					seenDps = <-sink.PointsChan
				}()
				resp, err := client.Do(req)
				So(err, ShouldBeNil)
				So(resp.StatusCode, ShouldEqual, 200)
				if reforwardToAHost {
					So(atomic.LoadInt64(&handler.reqCount), ShouldEqual, 1)
				}
				if expectDatapoints {
					<-seenDatapoints
					Convey("and all the metrics should parse", func() {
						So(len(seenDps), ShouldEqual, 18)
					})
				}
			}

			Convey("Sending a request should return 200-OK", func() {
				testAPost(examplePost, nil, true)
				Convey("and stats should come out", func() {
					So(len(l.Stats()), ShouldEqual, 13)
				})
			})

			Convey("Sending a request that doesn't deflate should error", func() {
				testAPost(examplePost, map[string]string{"Content-Encoding": "deflate"}, false)
				Convey("and the invalid parse should incr a variable", func() {
					So(metricFromDatapoints(l.Stats(), "error_decoding_compression").Value.(datapoint.IntValue).Int(), ShouldEqual, 1)
				})
			})

			Convey("Sending a compressed request should return 200-OK", func() {
				buf := bytes.Buffer{}
				w := zlib.NewWriter(&buf)
				io.Copy(w, bytes.NewBufferString(examplePost))
				w.Close()
				testAPost(buf.String(), map[string]string{"Content-Encoding": "deflate"}, true)
			})

			Convey("Sending an invalid req should 200-OK", func() {
				testAPost("__INVALID_JSON__", nil, false)
				Convey("and the invalid JSON should incr a variable", func() {
					So(metricFromDatapoints(l.Stats(), "error_decoding_json").Value.(datapoint.IntValue).Int(), ShouldEqual, 1)
				})
			})

			Reset(func() {
				server.Close()
				So(l.Close(), ShouldBeNil)
				close(sink.PointsChan)
			})
		}
	}

	Convey("With a normal server setup that does reforward", t, testWithReforward(true))
	Convey("With a normal server setup that does not reforward", t, testWithReforward(false))
}
