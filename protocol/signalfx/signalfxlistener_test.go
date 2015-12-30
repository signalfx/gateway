package signalfx

import (
	"bytes"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/pointer"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
	"io"
	"io/ioutil"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"
)

//
//import (
//	"bytes"
//	"encoding/json"
//	"errors"
//	"fmt"
//	"io"
//	"io/ioutil"
//	"net/http"
//	"net/http/httptest"
//	"sync/atomic"
//	"testing"
//	"time"
//
//	"github.com/cep21/gohelpers/workarounds"
//	"github.com/golang/protobuf/proto"
//	"github.com/signalfx/com_signalfx_metrics_protobuf"
//	"github.com/signalfx/metricproxy/config"
//	. "github.com/smartystreets/goconvey/convey"
//
//	"github.com/signalfx/golib/datapoint"
//	"github.com/signalfx/golib/datapoint/dptest"
//	"github.com/signalfx/golib/nettest"
//
//	"runtime"
//
//	"github.com/signalfx/golib/event"
//	"github.com/signalfx/golib/log"
//	"github.com/stretchr/testify/assert"
//	"golang.org/x/net/context"
//)
//
//func TestInvalidForwarderLoader(t *testing.T) {
//	listenFrom := &config.ListenFrom{
//		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:999999"),
//	}
//	sendTo := dptest.NewBasicSink()
//	ctx := context.Background()
//	_, err := ListenerLoader(ctx, sendTo, listenFrom, log.Discard)
//	assert.NotEqual(t, nil, err, "Should get an error making")
//}
//
//type myReaderType struct{}
//
//func (reader *myReaderType) Read(b []byte) (int, error) {
//	return 0, errors.New("can not read")
//}
//

var errReadErr = errors.New("could not read")

type errorReader struct{}

func (errorReader *errorReader) Read([]byte) (int, error) {
	return 0, errReadErr
}

//
//func localSetup(t *testing.T) (*ListenerServer, *dptest.BasicSink, string) {
//	sendTo := dptest.NewBasicSink()
//	ctx := context.Background()
//	server, err := StartServingHTTPOnPort(ctx, sendTo, "127.0.0.1:0", time.Second, "test_server", log.Discard)
//	baseURI := fmt.Sprintf("http://127.0.0.1:%d", nettest.TCPPort(server.listener))
//	assert.NotNil(t, server)
//	assert.NoError(t, err)
//	return server, sendTo, baseURI
//}
//
//func verifyFailure(t *testing.T, method string, baseURI string, contentType string, path string, body io.Reader) {
//	req, err := http.NewRequest(method, baseURI+path, body)
//	assert.NoError(t, err)
//	client := &http.Client{}
//	doneSignal := make(chan struct{})
//	if contentType != "" {
//		req.Header.Add("Content-Type", contentType)
//	}
//	resp, err := client.Do(req)
//	assert.NoError(t, err)
//	assert.NotEqual(t, http.StatusOK, resp.StatusCode)
//	assert.Equal(t, 0, len(doneSignal))
//}
//
//func verifyRequest(t *testing.T, baseURI string, contentType string, path string, body io.Reader, channel *dptest.BasicSink, metricName string, metricValue datapoint.Value) {
//	req, err := http.NewRequest("POST", baseURI+path, body)
//	assert.NoError(t, err)
//	client := &http.Client{}
//	doneSignal := make(chan struct{})
//	var dpOut *datapoint.Datapoint
//	go func() {
//		dpOut = channel.Next()
//		doneSignal <- struct{}{}
//	}()
//	if contentType != "" {
//		req.Header.Add("Content-Type", contentType)
//	}
//	resp, err := client.Do(req)
//	assert.NoError(t, err)
//	assert.Equal(t, http.StatusOK, resp.StatusCode)
//	_ = <-doneSignal
//	assert.Equal(t, metricName, dpOut.Metric)
//	assert.EqualValues(t, metricValue, dpOut.Value)
//}
//
//func TestConstTypeGetter(t *testing.T) {
//	assert.Equal(t, com_signalfx_metrics_protobuf.MetricType_GAUGE, ConstTypeGetter(com_signalfx_metrics_protobuf.MetricType_GAUGE).GetMetricTypeFromMap(""))
//}
//
//func TestSignalfxJsonV1Handler(t *testing.T) {
//	listener, channel, baseURI := localSetup(t)
//	defer listener.Close()
//	// Test no content type
//	body := bytes.NewBuffer([]byte(`{"metric":"ametric", "source":"asource", "value" : 3}{}`))
//	verifyRequest(t, baseURI, "", "/v1/datapoint", body, channel, "ametric", datapoint.NewIntValue(3))
//	// Test w/ json content type
//	body = bytes.NewBuffer([]byte(`{"metric":"ametric3", "source":"asource", "value" : 4.3}{}`))
//	verifyRequest(t, baseURI, "application/json", "/datapoint", body, channel, "ametric3", datapoint.NewFloatValue(4.3))
//}
//
//func TestSignalfxJSONV1Decoder(t *testing.T) {
//	decoder := JSONDecoderV1{}
//	req := &http.Request{
//		Body: ioutil.NopCloser(bytes.NewBuffer([]byte("INVALID_JSON"))),
//	}
//	ctx := context.Background()
//	e := decoder.Read(ctx, req)
//	assert.Error(t, e)
//}
//
//func oldBodyDp(dp *com_signalfx_metrics_protobuf.DataPoint) io.Reader {
//	dpInBytes, _ := proto.Marshal(dp)
//	varintBytes := proto.EncodeVarint(uint64(len(dpInBytes)))
//	body := bytes.NewBuffer(append(varintBytes, dpInBytes...))
//	return body
//}
//
//func TestSignalfxProtobufV1Handler(t *testing.T) {
//	listener, channel, baseURI := localSetup(t)
//	defer listener.Close()
//	protoDatapoint := &com_signalfx_metrics_protobuf.DataPoint{
//		Source: workarounds.GolangDoesnotAllowPointerToStringLiteral("asource"),
//		Metric: workarounds.GolangDoesnotAllowPointerToStringLiteral("ametric4"),
//		Value:  &com_signalfx_metrics_protobuf.Datum{IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(2)},
//	}
//	// Test int body
//	body := oldBodyDp(protoDatapoint)
//	verifyRequest(t, baseURI, "application/x-protobuf", "/v1/datapoint", body, channel, "ametric4", datapoint.NewIntValue(2))
//}
//
//func TestSignalfxProtobufV1Decoder(t *testing.T) {
//	typeGetter := metricHandler{
//		metricCreationsMap: make(map[string]com_signalfx_metrics_protobuf.MetricType),
//	}
//	sendTo := dptest.NewBasicSink()
//	ctx := context.Background()
//
//	decoder := ProtobufDecoderV1{
//		TypeGetter: &typeGetter,
//		Sink:       sendTo,
//		Logger:     log.Discard,
//	}
//	req := &http.Request{
//		Body: ioutil.NopCloser(bytes.NewBuffer([]byte("INVALID_PROTOBUF"))),
//	}
//	e := decoder.Read(ctx, req)
//	assert.Error(t, e)
//
//	varintBytes := proto.EncodeVarint(uint64(100))
//	req = &http.Request{
//		Body: ioutil.NopCloser(bytes.NewBuffer(varintBytes)),
//	}
//	e = decoder.Read(ctx, req)
//	assert.Error(t, e, "Should get error without anything left to read")
//
//	varintBytes = proto.EncodeVarint(uint64(0))
//	req = &http.Request{
//		Body: ioutil.NopCloser(bytes.NewBuffer(append(varintBytes, []byte("asfasdfsda")...))),
//	}
//	e = decoder.Read(ctx, req)
//	assert.Error(t, e, "Should get error reading len zero")
//
//	varintBytes = proto.EncodeVarint(uint64(200000))
//	req = &http.Request{
//		Body: ioutil.NopCloser(bytes.NewBuffer(append(varintBytes, []byte("abasdfsadfafdsc")...))),
//	}
//	e = decoder.Read(ctx, req)
//	assert.Equal(t, errProtobufTooLarge, e, "Should get error reading len zero")
//
//	varintBytes = proto.EncodeVarint(uint64(999999999))
//	req = &http.Request{
//		Body: ioutil.NopCloser(bytes.NewBuffer([]byte{byte(255), byte(147), byte(235), byte(235), byte(235)})),
//	}
//	e = decoder.Read(ctx, req)
//	assert.Equal(t, errInvalidProtobufVarint, e)
//
//	req = &http.Request{
//		Body: ioutil.NopCloser(&errorReader{}),
//	}
//	e = decoder.Read(ctx, req)
//	assert.Error(t, e, "Should get error reading if reader fails")
//
//	varintBytes = proto.EncodeVarint(uint64(20))
//	req = &http.Request{
//		Body: ioutil.NopCloser(bytes.NewBuffer(append(varintBytes, []byte("01234567890123456789")...))),
//	}
//	e = decoder.Read(ctx, req)
//	assert.Contains(t, e.Error(), "proto")
//
//	assert.Equal(t, 0, len(sendTo.PointsChan))
//}
//
//func TestSignalfxJSONV2Decoder(t *testing.T) {
//	decoder := JSONDecoderV2{}
//	req := &http.Request{
//		Body: ioutil.NopCloser(bytes.NewBuffer([]byte("INVALID_JSON"))),
//	}
//	req.ContentLength = -1
//	ctx := context.Background()
//	e := decoder.Read(ctx, req)
//	_ = e.(*json.SyntaxError)
//}
//
//func TestSignalfxJsonV2Handler(t *testing.T) {
//	listener, channel, baseURI := localSetup(t)
//	defer listener.Close()
//	body := bytes.NewBuffer([]byte(`{"unused":[], "gauge":[{"metric":"noval", "value":{"a":"b"}}, {"metric":"metrictwo", "value": 3}]}`))
//	verifyRequest(t, baseURI, "application/json", "/v2/datapoint", body, channel, "metrictwo", datapoint.NewIntValue(3))
//	body = bytes.NewBuffer([]byte(`{"unused":[], "gauge":[{"metric":"noval", "value":{"a":"b"}}, {"metric":"metrictwo", "value": 3}]}`))
//	verifyRequest(t, baseURI, "", "/v2/datapoint", body, channel, "metrictwo", datapoint.NewIntValue(3))
//}
//
//
//func TestSignalfxProtoV2Handler(t *testing.T) {
//	listener, channel, baseURI := localSetup(t)
//	defer listener.Close()
//
//	protoDatapoint := &com_signalfx_metrics_protobuf.DataPoint{
//		Source: workarounds.GolangDoesnotAllowPointerToStringLiteral("asource"),
//		Metric: workarounds.GolangDoesnotAllowPointerToStringLiteral("ametric4"),
//		Value:  &com_signalfx_metrics_protobuf.Datum{IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(2)},
//	}
//	uploadMsg := &com_signalfx_metrics_protobuf.DataPointUploadMessage{
//		Datapoints: []*com_signalfx_metrics_protobuf.DataPoint{protoDatapoint},
//	}
//	dpInBytes, _ := proto.Marshal(uploadMsg)
//	body := bytes.NewBuffer(dpInBytes)
//	verifyRequest(t, baseURI, "application/x-protobuf", "/v2/datapoint", body, channel, "ametric4", datapoint.NewIntValue(2))
//}
//
//func TestMetricHandler(t *testing.T) {
//	handler := metricHandler{
//		metricCreationsMap: make(map[string]com_signalfx_metrics_protobuf.MetricType),
//		logger:             log.Discard,
//	}
//	assert.Equal(t, com_signalfx_metrics_protobuf.MetricType_GAUGE, handler.GetMetricTypeFromMap("test"))
//	handler.metricCreationsMap["test"] = com_signalfx_metrics_protobuf.MetricType_COUNTER
//	assert.Equal(t, com_signalfx_metrics_protobuf.MetricType_COUNTER, handler.GetMetricTypeFromMap("test"))
//
//	// Testing OK write
//	rw := httptest.NewRecorder()
//	d := []MetricCreationStruct{}
//	b, err := json.Marshal(d)
//	assert.NoError(t, err)
//	w := bytes.NewBuffer(b)
//	req := &http.Request{
//		Body: ioutil.NopCloser(w),
//	}
//	handler.ServeHTTP(rw, req)
//	assert.Equal(t, "[]", rw.Body.String())
//	assert.Equal(t, rw.Code, http.StatusOK)
//
//	// Testing invalid metric write
//	rw = httptest.NewRecorder()
//	d = []MetricCreationStruct{{MetricType: "Invalid"}}
//	b, err = json.Marshal(d)
//	assert.NoError(t, err)
//	w = bytes.NewBuffer(b)
//	req = &http.Request{
//		Body: ioutil.NopCloser(w),
//	}
//	handler.ServeHTTP(rw, req)
//	assert.Contains(t, rw.Body.String(), "Invalid metric type")
//	assert.Equal(t, rw.Code, http.StatusBadRequest)
//
//	// Testing valid metric write
//	rw = httptest.NewRecorder()
//	d = []MetricCreationStruct{{MetricType: "COUNTER", MetricName: "ametric"}}
//	b, err = json.Marshal(d)
//	assert.NoError(t, err)
//	w = bytes.NewBuffer(b)
//	req = &http.Request{
//		Body: ioutil.NopCloser(w),
//	}
//	handler.ServeHTTP(rw, req)
//	assert.Equal(t, rw.Code, http.StatusOK)
//
//	assert.Equal(t, com_signalfx_metrics_protobuf.MetricType_COUNTER, handler.GetMetricTypeFromMap("ametric"))
//
//	rw = httptest.NewRecorder()
//	handler.jsonMarshal = func(v interface{}) ([]byte, error) {
//		return nil, fmt.Errorf("unable to marshal")
//	}
//	w = bytes.NewBuffer(b)
//	req = &http.Request{
//		Body: ioutil.NopCloser(w),
//	}
//	handler.ServeHTTP(rw, req)
//	assert.Equal(t, rw.Code, http.StatusBadRequest)
//	assert.Contains(t, rw.Body.String(), "Unable to marshal")
//}
//
//func TestStatSize(t *testing.T) {
//	listener, _, _ := localSetup(t)
//	defer listener.Close()
//	assert.Equal(t, 81, len(listener.Stats()))
//}
//
//func TestInvalidContentType(t *testing.T) {
//	listener, _, baseURI := localSetup(t)
//	defer listener.Close()
//	// Test no content type
//	body := bytes.NewBuffer([]byte(`{"metric":"ametric", "source":"asource", "value" : 3}{}`))
//	verifyFailure(t, "GET", baseURI, "application/json", "/datapoint", body)
//	verifyFailure(t, "GET", baseURI, "", "/datapoint", body)
//	verifyFailure(t, "POST", baseURI, "INVALID_TYPE", "/v1/datapoint", body)
//	verifyFailure(t, "POST", baseURI, "INVALID_TYPE", "/datapoint", body)
//	verifyFailure(t, "POST", baseURI, "INVALID_TYPE", "/v2/datapoint", body)
//	verifyFailure(t, "POST", baseURI, "INVALID_TYPE", "/v2/event", body)
//	verifyFailure(t, "POST", baseURI, "INVALID_TYPE", "/v1/collectd", body)
//	verifyFailure(t, "POST", baseURI, "INVALID_TYPE", "/metric", body)
//	verifyFailure(t, "POST", baseURI, "INVALID_TYPE", "/v1/metric", body)
//}
//
//type errTest string
//
//func (e errTest) Read(_ context.Context, _ *http.Request) error {
//	s := string(e)
//	if s == "" {
//		return nil
//	}
//	return fmt.Errorf(s)
//}
//
//func TestErrorTrackerHandler(t *testing.T) {
//	e := ErrorTrackerHandler{
//		reader: errTest(""),
//	}
//	ctx := context.Background()
//	rw := httptest.NewRecorder()
//	e.ServeHTTPC(ctx, rw, nil)
//	assert.Equal(t, int64(0), e.TotalErrors)
//	assert.Equal(t, `"OK"`, rw.Body.String())
//	e.reader = errTest("hi")
//	rw = httptest.NewRecorder()
//	e.ServeHTTPC(ctx, rw, nil)
//	assert.Equal(t, `hi`, rw.Body.String())
//	assert.Equal(t, int64(1), e.TotalErrors)
//}
//
//func TestSignalfxListener(t *testing.T) {
//	Convey("given a signalfx listener", t, func() {
//		body := bytes.NewBuffer([]byte(`[{"eventType": "mwp.test2", "dimensions": {"instance": "mwpinstance4", "host": "myhost-4", "service": "myservice4"}, "properties": {"version": "2015-11-23-4"}}]`))
//		Convey("verify the timestamp is filled in and the category becomes USER_DEFINED", func() {
//			verifyEventRequest(baseURI, "application/json", "/v2/event", body, channel, "mwp.test2", "USER_DEFINED", map[string]string{"instance": "mwpinstance4", "host": "myhost-4", "service": "myservice4"}, map[string]interface{}{"version": "2015-11-23-4"}, nil)
//		})
//	})
//}
//
//func TestBob(t *testing.T) {
//	Convey("Given json decoder show invalid json errors", t, func() {
//		decoder := JSONEventDecoderV2{}
//		req := &http.Request{
//			Body: ioutil.NopCloser(strings.NewReader("INVALID_JSON")),
//		}
//		req.ContentLength = -1
//		ctx := context.Background()
//		e := decoder.Read(ctx, req)
//		_ = e.(*json.SyntaxError)
//	})
//}

//func TestSignalfxJsonV2EventHandler(t *testing.T) {
//	// default for category and timestamp

//
//	Convey("given a protobuf body without or category", t, func() {
//		protoEvent := &com_signalfx_metrics_protobuf.Event{
//			EventType: workarounds.GolangDoesnotAllowPointerToStringLiteral("mwp.test2"),
//			Dimensions: []*com_signalfx_metrics_protobuf.Dimension{
//				{
//					Key:   workarounds.GolangDoesnotAllowPointerToStringLiteral("instance"),
//					Value: workarounds.GolangDoesnotAllowPointerToStringLiteral("mwpinstance4"),
//				},
//				{
//					Key:   workarounds.GolangDoesnotAllowPointerToStringLiteral("host"),
//					Value: workarounds.GolangDoesnotAllowPointerToStringLiteral("myhost-4"),
//				},
//				{
//					Key:   workarounds.GolangDoesnotAllowPointerToStringLiteral("service"),
//					Value: workarounds.GolangDoesnotAllowPointerToStringLiteral("myservice4"),
//				},
//			},
//			Properties: []*com_signalfx_metrics_protobuf.Property{
//				{
//					Key: workarounds.GolangDoesnotAllowPointerToStringLiteral("version"),
//					Value: &com_signalfx_metrics_protobuf.PropertyValue{
//						StrValue: workarounds.GolangDoesnotAllowPointerToStringLiteral("2015-11-23-4"),
//					},
//				},
//				{
//					Key: workarounds.GolangDoesnotAllowPointerToStringLiteral("int"),
//					Value: &com_signalfx_metrics_protobuf.PropertyValue{
//						IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(4),
//					},
//				},
//				{
//					Key: workarounds.GolangDoesnotAllowPointerToStringLiteral("bool"),
//					Value: &com_signalfx_metrics_protobuf.PropertyValue{
//						BoolValue: workarounds.GolangDoesnotAllowPointerToBooleanLiteral(true),
//					},
//				},
//				{
//					Key: workarounds.GolangDoesnotAllowPointerToStringLiteral("double"),
//					Value: &com_signalfx_metrics_protobuf.PropertyValue{
//						DoubleValue: workarounds.GolangDoesnotAllowPointerToFloat64Literal(1.1),
//					},
//				},
//			},
//		}
//		uploadMsg := &com_signalfx_metrics_protobuf.EventUploadMessage{
//			Events: []*com_signalfx_metrics_protobuf.Event{protoEvent},
//		}
//		dpInBytes, _ := proto.Marshal(uploadMsg)
//		body := bytes.NewBuffer(dpInBytes)
//		Convey("verify the timestamp is acurate the category is USER_DEFINED", func() {
//			verifyEventRequest(baseURI, "application/x-protobuf", "/v2/event", body, channel, "mwp.test2", "USER_DEFINED", map[string]string{"instance": "mwpinstance4", "host": "myhost-4", "service": "myservice4"}, map[string]interface{}{"version": "2015-11-23-4", "int": int64(4), "bool": true, "double": 1.1}, nil)
//		})
//	})
//
//	Convey("given a protobuf event decoder", t, func() {
//		decoder := ProtobufEventDecoderV2{Logger: log.Discard}
//		ctx := context.Background()
//		Convey("show an empty request errors", func() {
//			req := &http.Request{
//				Body: ioutil.NopCloser(bytes.NewBufferString("")),
//			}
//			req.ContentLength = -1
//			So(decoder.Read(ctx, req), ShouldEqual, errInvalidContentLength)
//		})
//		Convey("show an error reading errors", func() {
//			req := &http.Request{
//				Body: ioutil.NopCloser(&errorReader{}),
//			}
//			req.ContentLength = 1
//			e := decoder.Read(ctx, req)
//			So(e.Error(), ShouldEqual, "could not read")
//		})
//		Convey("show invalid json errors", func() {
//			req := &http.Request{
//				Body: ioutil.NopCloser(bytes.NewBufferString("INVALID_JSON")),
//			}
//			req.ContentLength = int64(len("INVALID_JSON"))
//			e := decoder.Read(ctx, req)
//			So(e.Error(), ShouldEqual, "proto: can't skip unknown wire type 7 for com_signalfx_metrics_protobuf.EventUploadMessage")
//		})
//	})
//}
//

func TestSignalfxProtoDecoders(t *testing.T) {
	readerCheck := func(decoder ErrorReader) {
		Convey("Should expect invalid content length", func() {
			req := &http.Request{
				Body: ioutil.NopCloser(bytes.NewReader(nil)),
			}
			req.ContentLength = -1
			ctx := context.Background()
			So(decoder.Read(ctx, req), ShouldEqual, errInvalidContentLength)
		})
		Convey("should check read errors", func() {
			req := &http.Request{
				Body: ioutil.NopCloser(&errorReader{}),
			}
			req.ContentLength = 1
			ctx := context.Background()
			So(decoder.Read(ctx, req), ShouldEqual, errReadErr)
		})
	}
	Convey("a setup ProtobufDecoderV2", t, func() {
		decoder := ProtobufDecoderV2{Logger: log.Discard}
		readerCheck(&decoder)
	})
	Convey("a setup ProtobufEventDecoderV2", t, func() {
		decoder := ProtobufEventDecoderV2{Logger: log.Discard}
		readerCheck(&decoder)
	})
}

func TestSignalfxProtobufV1Decoder(t *testing.T) {
	Convey("a setup metric decoder", t, func() {
		typeGetter := metricHandler{
			metricCreationsMap: make(map[string]com_signalfx_metrics_protobuf.MetricType),
		}
		sendTo := dptest.NewBasicSink()
		ctx := context.Background()

		decoder := ProtobufDecoderV1{
			TypeGetter: &typeGetter,
			Sink:       sendTo,
			Logger:     log.Discard,
		}
		Convey("Should expect invalid protobufs", func() {
			req := &http.Request{
				Body: ioutil.NopCloser(strings.NewReader("INVALID_PROTOBUF")),
			}
			So(decoder.Read(ctx, req), ShouldNotBeNil)
		})
		Convey("Should get error without anything left to read", func() {
			varintBytes := proto.EncodeVarint(uint64(100))
			req := &http.Request{
				Body: ioutil.NopCloser(bytes.NewReader(varintBytes)),
			}
			So(decoder.Read(ctx, req), ShouldNotBeNil)
		})
		Convey("Should get error reading len zero", func() {
			varintBytes := proto.EncodeVarint(uint64(0))
			req := &http.Request{
				Body: ioutil.NopCloser(bytes.NewReader(append(varintBytes, []byte("asfasdfsda")...))),
			}
			So(decoder.Read(ctx, req), ShouldNotBeNil)
		})

		Convey("Should get error reading big lengths", func() {
			varintBytes := proto.EncodeVarint(uint64(200000))
			req := &http.Request{
				Body: ioutil.NopCloser(bytes.NewReader(append(varintBytes, []byte("abasdfsadfafdsc")...))),
			}
			So(decoder.Read(ctx, req), ShouldEqual, errProtobufTooLarge)
		})

		Convey("Should get error reading invalid varints", func() {
			req := &http.Request{
				Body: ioutil.NopCloser(bytes.NewReader([]byte{byte(255), byte(147), byte(235), byte(235), byte(235)})),
			}
			So(decoder.Read(ctx, req), ShouldEqual, errInvalidProtobufVarint)
		})
		Convey("Should get error reading invalid protobuf after varint", func() {
			varintBytes := proto.EncodeVarint(uint64(20))
			req := &http.Request{
				Body: ioutil.NopCloser(bytes.NewReader(append(varintBytes, []byte("01234567890123456789")...))),
			}
			So(decoder.Read(ctx, req).Error(), ShouldContainSubstring, "proto")
		})

		//		varintBytes = proto.EncodeVarint(uint64(999999999))
		//		req = &http.Request{
		//			Body: ioutil.NopCloser(bytes.NewBuffer([]byte{byte(255), byte(147), byte(235), byte(235), byte(235)})),
		//		}
		//		e = decoder.Read(ctx, req)
		//		assert.Equal(t, errInvalidProtobufVarint, e)
		//
		//		req = &http.Request{
		//			Body: ioutil.NopCloser(&errorReader{}),
		//		}
		//		e = decoder.Read(ctx, req)
		//		assert.Error(t, e, "Should get error reading if reader fails")
		//
		//		varintBytes = proto.EncodeVarint(uint64(20))
		//		req = &http.Request{
		//			Body: ioutil.NopCloser(bytes.NewBuffer(append(varintBytes, []byte("01234567890123456789")...))),
		//		}
		//		e = decoder.Read(ctx, req)
		//		assert.Contains(t, e.Error(), "proto")
		//
		//		assert.Equal(t, 0, len(sendTo.PointsChan))
	})
}

func verifyEventRequest(baseURI string, contentType string, path string, body io.Reader, channel *dptest.BasicSink, eventType string, category string, dimensions map[string]string, properties map[string]interface{}, reqErr error) {
	Convey("given a new request with path "+path, func() {
		req, err := http.NewRequest("POST", baseURI+path, body)
		if reqErr != nil {
			Convey("we should get the error provided"+reqErr.Error(), func() {
				So(err, ShouldNotBeNil)
				So(reqErr, ShouldEqual, err)
			})
		} else {
			Convey("we should be able to successfully parse the payload", func() {
				So(err, ShouldBeNil)
				doneSignal := make(chan struct{})
				var eOut *event.Event
				go func() {
					eOut = channel.NextEvent()
					doneSignal <- struct{}{}
				}()
				client := &http.Client{}
				if contentType != "" {
					req.Header.Add("Content-Type", contentType)
				}
				resp, err := client.Do(req)
				So(err, ShouldBeNil)
				So(resp.StatusCode, ShouldEqual, http.StatusOK)
				Convey("and the generated event should be what we expect", func() {
					runtime.Gosched()
					_ = <-doneSignal
					So(eventType, ShouldEqual, eOut.EventType)
					So(category, ShouldEqual, eOut.Category)
					if dimensions != nil {
						So(dimensions, ShouldResemble, eOut.Dimensions)
					}
					if properties != nil {
						So(properties, ShouldResemble, eOut.Meta)
					}
					So(eOut.Timestamp.Nanosecond(), ShouldBeGreaterThan, 0)
				})
			})
		}
	})
}

func TestSignalfxListenerFailure(t *testing.T) {
	Convey("invalid addr should not listen", t, func() {
		listenConf := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:9999999999"),
		}
		_, err := NewListener(nil, listenConf)
		So(err, ShouldNotBeNil)
	})
}

func TestMapToProperties(t *testing.T) {
	if len(mapToProperties(map[string]interface{}{"test": errors.New("unknown")})) > 0 {
		t.Error("Unexpected map length")
	}
}

func TestCheckResp(t *testing.T) {
	Convey("With a resp", t, func() {
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(strings.NewReader(`"OK"`)),
		}
		Convey("I should check read errors", func() {
			resp.Body = ioutil.NopCloser(&errorReader{})
			So(errors.Details(checkResp(resp)), ShouldContainSubstring, "unable to verify response body")
		})
		Convey("I should check status code", func() {
			resp.StatusCode = http.StatusNotFound
			So(checkResp(resp).Error(), ShouldContainSubstring, "invalid status code")
		})
		Convey("I should check for invalid JSON", func() {
			resp.Body = ioutil.NopCloser(strings.NewReader(`INVALID_JSON`))
			So(checkResp(resp), ShouldNotBeNil)
		})
		Convey("I should check for valid JSON that is not OK", func() {
			resp.Body = ioutil.NopCloser(strings.NewReader(`"NOT_OK"`))
			So(errors.Details(checkResp(resp)), ShouldContainSubstring, "Resp body not ok")
		})
	})
}

func TestSignalfxListener(t *testing.T) {
	Convey("given a signalfx listener", t, func() {
		sendTo := dptest.NewBasicSink()
		sendTo.Resize(1)
		ctx := context.Background()
		logBuf := &bytes.Buffer{}
		logger := log.NewLogfmtLogger(logBuf, log.Panic)
		listenConf := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:0"),
			Logger:     logger,
		}
		listener, err := NewListener(sendTo, listenConf)
		So(err, ShouldBeNil)
		baseURI := fmt.Sprintf("http://127.0.0.1:%d", nettest.TCPPort(listener.listener))
		Convey("And a signalfx forwarder", func() {
			forwardConfig := &ForwarderConfig{
				DatapointURL: pointer.String(fmt.Sprintf("%s/v2/datapoint", baseURI)),
				EventURL:     pointer.String(fmt.Sprintf("%s/v2/event", baseURI)),
				Logger:       logger,
			}
			forwarder := NewForwarder(forwardConfig)
			Convey("event post to nowhere should fail", func() {
				forwarder.eventURL = "http://localhost:1"
				So(forwarder.AddEvents(ctx, []*event.Event{dptest.E()}), ShouldNotBeNil)
			})
			Convey("Should be able to send a point", func() {
				dpSent := dptest.DP()
				dpSent.Timestamp = dpSent.Timestamp.Round(time.Millisecond)
				So(forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dpSent}), ShouldBeNil)
				dpSeen := sendTo.Next()
				So(dpSent.String(), ShouldEqual, dpSeen.String())
			})
			Convey("Should be able to send zero len events", func() {
				So(forwarder.AddEvents(ctx, []*event.Event{}), ShouldBeNil)
			})
			Convey("Should be able to send zero len datapoints", func() {
				So(forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{}), ShouldBeNil)
			})
			Convey("Should check event marshal failures", func() {
				forwarder.protoMarshal = func(pb proto.Message) ([]byte, error) {
					return nil, errors.New("nope")
				}
				So(forwarder.AddEvents(ctx, []*event.Event{dptest.E()}), ShouldNotBeNil)
			})
			Convey("Should be able to send events", func() {
				eventSent := dptest.E()
				eventSent.Timestamp = eventSent.Timestamp.Round(time.Millisecond)
				eventSent.Dimensions["..."] = "testing"
				So(forwarder.AddEvents(ctx, []*event.Event{eventSent}), ShouldBeNil)
				eventSeen := sendTo.NextEvent()
				eventSent.Dimensions["___"] = "testing" // dots should turn into _
				delete(eventSent.Dimensions, "...")
				So(eventSeen.Dimensions, ShouldResemble, eventSent.Dimensions)
				So(eventSeen.Meta, ShouldResemble, eventSent.Meta)
				eventSeen.Dimensions = nil
				eventSent.Dimensions = nil
				eventSeen.Meta = nil
				eventSent.Meta = nil
				So(eventSeen.String(), ShouldEqual, eventSent.String())
			})
			Convey("Should filter event properties/dimensions that are empty", func() {
				eventSent := dptest.E()
				eventSent.Dimensions = map[string]string{"": "test"}
				eventSent.Meta = map[string]interface{}{"": "test"}
				eventSent.Timestamp = eventSent.Timestamp.Round(time.Millisecond)
				So(forwarder.AddEvents(ctx, []*event.Event{eventSent}), ShouldBeNil)
				eventSeen := sendTo.NextEvent()
				eventSent.Dimensions = nil
				eventSent.Meta = nil
				So(eventSeen.String(), ShouldEqual, eventSent.String())
			})
		})

		trySend := func(body string, contentType string, pathSuffix string) {
			client := http.Client{}
			req, err := http.NewRequest("POST", baseURI+pathSuffix, strings.NewReader(body))
			So(err, ShouldBeNil)
			req.Header.Add("Content-Type", contentType)
			resp, err := client.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
		}
		verifyStatusCode := func(body string, contentType string, pathSuffix string, expectedStatusCode int) {
			client := http.Client{}
			req, err := http.NewRequest("POST", baseURI+pathSuffix, strings.NewReader(body))
			So(err, ShouldBeNil)
			req.Header.Add("Content-Type", contentType)
			resp, err := client.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, expectedStatusCode)
		}
		Convey("given a json body without a timestamp or category", func() {
			body := bytes.NewBuffer([]byte(`[{"eventType": "mwp.test2", "dimensions": {"instance": "mwpinstance4", "host": "myhost-4", "service": "myservice4"}, "properties": {"version": "2015-11-23-4"}}]`))
			Convey("verify the timestamp is filled in and the category becomes USER_DEFINED", func() {
				verifyEventRequest(baseURI, "application/json", "/v2/event", body, sendTo, "mwp.test2", "USER_DEFINED", map[string]string{"instance": "mwpinstance4", "host": "myhost-4", "service": "myservice4"}, map[string]interface{}{"version": "2015-11-23-4"}, nil)
			})
		})
		Convey("given a json body with a timestamp and category ALERT", func() {
			body := bytes.NewBuffer([]byte(`[{"category":"ALERT", "eventType": "mwp.test2", "dimensions": {"instance": "mwpinstance4", "host": "myhost-4", "service": "myservice4"}, "properties": {"version": "2015-11-23-4"}, "timestamp":12345}]`))
			Convey("verify the timestamp is acurate the category is ALERT", func() {
				verifyEventRequest(baseURI, "application/json", "/v2/event", body, sendTo, "mwp.test2", "ALERT", map[string]string{"instance": "mwpinstance4", "host": "myhost-4", "service": "myservice4"}, map[string]interface{}{"version": "2015-11-23-4"}, nil)
			})
		})
		Convey("Should be able to send a v2 JSON point", func() {
			now := time.Now().Round(time.Second)
			v1Body := fmt.Sprintf(`{"gauge": [{"metric":"bob", "value": 3, "timestamp": %d}]}`, now.UnixNano()/time.Millisecond.Nanoseconds())
			trySend(v1Body, "application/json", "/v2/datapoint")
			datapointSent := datapoint.Datapoint{
				Metric:    "bob",
				Value:     datapoint.NewIntValue(3),
				Timestamp: now,
			}
			datapointSeen := sendTo.Next()
			So(datapointSent.String(), ShouldEqual, datapointSeen.String())
		})
		Convey("invalid requests should error", func() {
			verifyStatusCode("INVALID_JSON", "application/json", "/v1/datapoint", http.StatusBadRequest)
			verifyStatusCode("INVALID_PROTOBUF", "application/x-protobuf", "/v1/datapoint", http.StatusBadRequest)
			verifyStatusCode("INVALID_JSON", "application/json", "/v2/datapoint", http.StatusBadRequest)

			verifyStatusCode("INVALID_JSON", "application/json", "/v1/metric", http.StatusBadRequest)
			verifyStatusCode(`[{"sf_metric":"bob", "sf_metricType":"GAU_GE"}]`, "application/json", "/v1/metric", http.StatusBadRequest)
			verifyStatusCode("INVALID_JSON", "application/json", "/v2/event", http.StatusBadRequest)
			verifyStatusCode("INVALID_PROTOBUF", "application/x-protobuf", "/v2/event", http.StatusBadRequest)

			verifyStatusCode("INVALID_JSON", "application/json", "/v1/event", http.StatusNotFound)

			dps := listener.Datapoints()
			So(dptest.ExactlyOneDims(dps, "total_errors", map[string]string{"type": "sfx_protobuf_v2"}).Value.String(), ShouldEqual, "0")
			verifyStatusCode("INVALID_PROTOBUF", "application/x-protobuf", "/v2/datapoint", http.StatusBadRequest)
			dps = listener.Datapoints()
			So(dptest.ExactlyOneDims(dps, "total_errors", map[string]string{"type": "sfx_protobuf_v2"}).Value.String(), ShouldEqual, "1")
		})
		Convey("Should be able to send a v2 JSON event", func() {
			now := time.Now().Round(time.Second)
			body := fmt.Sprintf(`[{"eventType":"ET", "category": "cat", "timestamp": %d}]`, now.UnixNano()/time.Millisecond.Nanoseconds())
			trySend(body, "application/json", "/v2/event")
			eventSent := event.Event{
				EventType: "ET",
				Category:  "cat",
				Timestamp: now,
			}
			eventSeen := sendTo.NextEvent()
			So(eventSent.String(), ShouldEqual, eventSeen.String())
		})
		Convey("Should be able to send a v1 metric type", func() {
			trySend(`[{"sf_metric":"bob", "sf_metricType": "GAUGE"}]`, "application/json", "/v1/metric")
			So(listener.metricHandler.GetMetricTypeFromMap("bob"), ShouldEqual, com_signalfx_metrics_protobuf.MetricType_GAUGE)
		})
		Convey("Should check v1 JSON errors", func() {
			listener.metricHandler.jsonMarshal = func(v interface{}) ([]byte, error) {
				return nil, errors.New("nope")
			}
			verifyStatusCode(`[{"sf_metric":"bob", "sf_metricType": "GAUGE"}]`, "application/json", "/v1/metric", http.StatusBadRequest)
		})
		Convey("Should be able to send a v1 JSON point", func() {
			datapointSent := datapoint.Datapoint{
				Metric:     "bob",
				Value:      datapoint.NewIntValue(3),
				Dimensions: map[string]string{"sf_source": "name"},
			}
			trySend(`{"metric":"bob", "source": "name", "value": 3}`, "application/json", "/v1/datapoint")
			datapointSeen := sendTo.Next()
			datapointSent.Timestamp = datapointSeen.Timestamp
			So(datapointSent.String(), ShouldEqual, datapointSeen.String())
		})
		Convey("Should ignore missing metric name for v1 json points", func() {
			trySend(`{"source": "name", "value": 3}`, "application/json", "/v1/datapoint")
			So(len(sendTo.PointsChan), ShouldEqual, 0)
		})
		Convey("Should ignore invalid types or value on /v2/datapoint", func() {
			v2Body := `{"gauge": [{"metric":"bob"}], "UNKNOWN":[]}`
			So(len(sendTo.PointsChan), ShouldEqual, 0)
			trySend(v2Body, "application/json", "/v2/datapoint")
			So(len(sendTo.PointsChan), ShouldEqual, 0)
		})
		Convey("Should be able to send a v1 protobuf point", func() {
			datapointSent := datapoint.Datapoint{
				Metric: "metric",
				Value:  datapoint.NewIntValue(3),
			}
			dp := &com_signalfx_metrics_protobuf.DataPoint{
				Metric: pointer.String("metric"),
				Value: &com_signalfx_metrics_protobuf.Datum{
					IntValue: pointer.Int64(3),
				},
			}
			dpBytes, err := proto.Marshal(dp)
			So(err, ShouldBeNil)
			body := bytes.Buffer{}
			body.Write(proto.EncodeVarint(uint64(len(dpBytes))))
			body.Write(dpBytes)
			trySend(body.String(), "application/x-protobuf", "/v1/datapoint")
			datapointSeen := sendTo.Next()
			datapointSent.Timestamp = datapointSeen.Timestamp
			So(datapointSent.String(), ShouldEqual, datapointSeen.String())
		})

		Reset(func() {
			So(listener.Close(), ShouldBeNil)
		})
	})
}
