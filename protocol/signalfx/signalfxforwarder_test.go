package signalfx
//
//import (
//	"fmt"
//	"testing"
//
//	"bytes"
//	"encoding/json"
//	"io/ioutil"
//	"net/http"
//	"time"
//
//	"github.com/cep21/gohelpers/workarounds"
//	"github.com/golang/protobuf/proto"
//	"github.com/signalfx/golib/datapoint"
//	"github.com/signalfx/golib/event"
//	"github.com/signalfx/golib/nettest"
//	"github.com/signalfx/metricproxy/config"
//
//	"net/http/httptest"
//
//	"errors"
//
//	"github.com/signalfx/golib/datapoint/dptest"
//	"github.com/signalfx/golib/log"
//	. "github.com/smartystreets/goconvey/convey"
//	"github.com/stretchr/testify/assert"
//	"golang.org/x/net/context"
//)
//
//func TestBodySendFormat(t *testing.T) {
//	b := &BodySendFormatV2{
//		Metric: "cpu",
//	}
//	assert.Contains(t, b.String(), "cpu", "Expect cpu")
//}
//
//type metricPanicDatapoint struct {
//	datapoint.Datapoint
//}
//
//func (vp *metricPanicDatapoint) Metric() string {
//	panic("This shouldn't happen!")
//}
//
//func TestMapToProperties(t *testing.T) {
//	Convey("given a map of properties", t, func() {
//		p := map[string]interface{}{
//			"dim1": int(8),
//			"dim2": int64(3),
//		}
//		Convey("invalid properties are ignored", func() {
//			res := mapToProperties(p)
//			So(len(res), ShouldEqual, 1)
//		})
//	})
//}
//
//func TestMapToDimensions(t *testing.T) {
//	r := map[string]string{
//		"dim1":     "val1",
//		"dim2":     "val2",
//		"key:char": "val3",
//	}
//	res := mapToDimensions(r)
//	expect := make(map[string]string)
//	for _, d := range res {
//		expect[d.GetKey()] = d.GetValue()
//	}
//	delete(r, "key:char")
//	r["key_char"] = "val3"
//	assert.Equal(t, r, expect, "Dimensions don't parse right")
//	r["invalid_val"] = ""
//	res = mapToDimensions(r)
//	delete(r, "invalid_val")
//	assert.Equal(t, r, expect, "Dimensions don't parse right")
//}
//
//func TestFilterSignalfxString(t *testing.T) {
//	assert.Equal(t, "hello", filterSignalfxKey("hello"), "Filter not working")
//	assert.Equal(t, "_hello_bob1__", filterSignalfxKey(".hello:bob1_&"), "Filter not working")
//}
//
//func setupServerForwarder(t *testing.T) (*dptest.BasicSink, *ListenerServer, *Forwarder) {
//	// TODO: Break this out into smaller tests
//	listenFromSignalfx := config.ListenFrom{}
//	listenFromSignalfx.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0")
//
//	finalDatapointDestination := dptest.NewBasicSink()
//	ctx := context.Background()
//	l, err := ListenerLoader(ctx, finalDatapointDestination, &listenFromSignalfx, log.Discard)
//	assert.Equal(t, nil, err, "Expect no error")
//
//	port := nettest.TCPPort(l.listener)
//
//	forwardTo := config.ForwardTo{
//		URL:              workarounds.GolangDoesnotAllowPointerToStringLiteral(fmt.Sprintf("http://127.0.0.1:%d/v2/datapoint", port)),
//		TimeoutDuration:  workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 1),
//		DefaultAuthToken: workarounds.GolangDoesnotAllowPointerToStringLiteral("AUTH_TOKEN"),
//		DefaultSource:    workarounds.GolangDoesnotAllowPointerToStringLiteral("proxy-source"),
//		SourceDimensions: workarounds.GolangDoesnotAllowPointerToStringLiteral("username,ignored,hostname"),
//	}
//
//	_, forwarder, err := ForwarderLoader1(ctx, &forwardTo, log.Discard)
//	assert.NoError(t, err, "Expect no error")
//	return finalDatapointDestination, l, forwarder
//}
//
//func TestForwarderLoaderOldVersion(t *testing.T) {
//	forwardTo := config.ForwardTo{
//		FormatVersion:    workarounds.GolangDoesnotAllowPointerToUintLiteral(1),
//		DefaultAuthToken: workarounds.GolangDoesnotAllowPointerToStringLiteral("AUTH_TOKEN"),
//	}
//	ctx := context.Background()
//	_, err := ForwarderLoader(ctx, &forwardTo, log.Discard)
//	assert.Error(t, err)
//}
//
//func TestNoSource(t *testing.T) {
//	finalDatapointDestination, l, forwarder := setupServerForwarder(t)
//	defer l.Close()
//
//	forwarder.defaultSource = ""
//	timeToSend := time.Now().Round(time.Second)
//	dpSent := datapoint.New("metric", map[string]string{}, datapoint.NewIntValue(2), datapoint.Gauge, timeToSend)
//	go forwarder.AddDatapoints(context.Background(), []*datapoint.Datapoint{dpSent})
//	dpRecieved := finalDatapointDestination.Next()
//	i := dpRecieved.Value.(datapoint.IntValue).Int()
//	assert.Equal(t, int64(2), i, "Expect 2 back")
//	val, exists := dpRecieved.Dimensions["sf_source"]
//	assert.False(t, exists, val)
//}
//
//func TestConnectorProcessProtoError(t *testing.T) {
//	expectedErr := errors.New("marshal error")
//	f := Forwarder{
//		protoMarshal: func(pb proto.Message) ([]byte, error) {
//			return nil, expectedErr
//		},
//		jsonMarshal: func(interface{}) ([]byte, error) {
//			return nil, expectedErr
//		},
//	}
//	err := f.AddEvents(context.Background(), []*event.Event{dptest.E()})
//	assert.Equal(t, expectedErr, err.(*forwardError).originalError)
//}
//
//type roundTripTest func(r *http.Request) (*http.Response, error)
//
//func (r roundTripTest) RoundTrip(req *http.Request) (*http.Response, error) {
//	return r(req)
//}
//
//func TestClientReqError(t *testing.T) {
//	f := Forwarder{
//		protoMarshal: proto.Marshal,
//		client: &http.Client{
//			Transport: roundTripTest(func(r *http.Request) (*http.Response, error) {
//				return nil, fmt.Errorf("unable to execute http request")
//			}),
//		},
//	}
//	err := f.AddEvents(context.Background(), []*event.Event{dptest.E()})
//	assert.Contains(t, err.Error(), "unable to execute http request")
//}
//
//type readError struct {
//}
//
//func (r *readError) Read([]byte) (int, error) {
//	return 0, fmt.Errorf("read error")
//}
//
//func TestResponseBodyError(t *testing.T) {
//	f := Forwarder{
//		protoMarshal: proto.Marshal,
//		client: &http.Client{
//			Transport: roundTripTest(func(r *http.Request) (*http.Response, error) {
//				r2 := http.Response{
//					Body: ioutil.NopCloser(&readError{}),
//				}
//				return &r2, nil
//			}),
//		},
//	}
//	err := f.AddEvents(context.Background(), []*event.Event{dptest.E()})
//	assert.Equal(t, "read error", err.(*forwardError).originalError.Error())
//}
//
//func TestResponseBadStatus(t *testing.T) {
//	f := Forwarder{
//		protoMarshal: proto.Marshal,
//		client: &http.Client{
//			Transport: roundTripTest(func(r *http.Request) (*http.Response, error) {
//				r2 := http.Response{
//					Body:       ioutil.NopCloser(bytes.NewBufferString("")),
//					StatusCode: 404,
//				}
//				return &r2, nil
//			}),
//		},
//	}
//	err := f.AddEvents(context.Background(), []*event.Event{dptest.E()})
//	assert.Contains(t, err.(*forwardError).originalError.Error(), "invalid status code")
//}
//
//func TestAllInvalid(t *testing.T) {
//	dp := dptest.DP()
//	dp.Metric = ""
//	f := Forwarder{}
//	assert.NoError(t, f.AddDatapoints(context.Background(), []*datapoint.Datapoint{dp}))
//	assert.NoError(t, f.AddEvents(context.Background(), []*event.Event{}))
//}
//
//func TestResponseBadJSON(t *testing.T) {
//	f := Forwarder{
//		protoMarshal: proto.Marshal,
//		jsonMarshal:  json.Marshal,
//		client: &http.Client{
//			Transport: roundTripTest(func(r *http.Request) (*http.Response, error) {
//				r2 := http.Response{
//					Body:       ioutil.NopCloser(bytes.NewBufferString("INVALID_JSON")),
//					StatusCode: 200,
//				}
//				return &r2, nil
//			}),
//		},
//	}
//	err := f.AddEvents(context.Background(), []*event.Event{dptest.E()})
//	assert.IsType(t, &json.SyntaxError{}, err.(*forwardError).originalError)
//}
//
//func TestResponseBadBody(t *testing.T) {
//	f := Forwarder{
//		protoMarshal: proto.Marshal,
//		jsonMarshal:  json.Marshal,
//		client: &http.Client{
//			Transport: roundTripTest(func(r *http.Request) (*http.Response, error) {
//				r2 := http.Response{
//					Body:       ioutil.NopCloser(bytes.NewBufferString(`"BAD"`)),
//					StatusCode: 200,
//				}
//				return &r2, nil
//			}),
//		},
//	}
//	err := f.AddEvents(context.Background(), []*event.Event{dptest.E()})
//	assert.Contains(t, err.Error(), "body decode error")
//}
//
//func TestBasicSend(t *testing.T) {
//	Convey("given an test server and forwarder", t, func() {
//		testServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
//			assert.Equal(t, "abcd", req.Header.Get("User-Agent"))
//			assert.Equal(t, "abcdefg", req.Header.Get(TokenHeaderName))
//			rw.Write([]byte(`"OK"`))
//		}))
//		defer testServer.Close()
//
//		f := NewSignalfxJSONForwarder("", time.Second, "", 10, "", "", "")
//		f.EventEndpoint(testServer.URL)
//		f.defaultAuthToken = "abcdefg"
//		f.userAgent = "abcd"
//		ctx := context.Background()
//		Convey("a valid event", func() {
//			e := dptest.E()
//			Convey("should not error", func() {
//				So(f.AddEvents(ctx, []*event.Event{e}), ShouldBeNil)
//			})
//		})
//	})
//}
