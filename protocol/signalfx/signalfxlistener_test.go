package signalfx

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/golang/protobuf/proto"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/metricproxy/config"

	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/datapoint/dptest"
	"github.com/signalfx/metricproxy/nettest"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestInvalidForwarderLoader(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:999999"),
	}
	sendTo := dptest.NewBasicSink()
	ctx := context.Background()
	_, err := ListenerLoader(ctx, sendTo, listenFrom)
	assert.NotEqual(t, nil, err, "Should get an error making")
}

type myReaderType struct{}

func (reader *myReaderType) Read(b []byte) (int, error) {
	return 0, errors.New("can not read")
}

type errorReader struct{}

func (errorReader *errorReader) Read([]byte) (int, error) {
	return 0, errors.New("could not read")
}

func localSetup(t *testing.T) (*ListenerServer, *dptest.BasicSink, string) {
	sendTo := dptest.NewBasicSink()
	ctx := context.Background()
	server, err := StartServingHTTPOnPort(ctx, sendTo, "127.0.0.1:0", time.Second, "test_server")
	baseURI := fmt.Sprintf("http://127.0.0.1:%d", nettest.TCPPort(server.listener))
	assert.NotNil(t, server)
	assert.NoError(t, err)
	return server, sendTo, baseURI
}

func verifyFailure(t *testing.T, method string, baseURI string, contentType string, path string, body io.Reader) {
	req, err := http.NewRequest(method, baseURI+path, body)
	assert.NoError(t, err)
	client := &http.Client{}
	doneSignal := make(chan struct{})
	if contentType != "" {
		req.Header.Add("Content-Type", contentType)
	}
	resp, err := client.Do(req)
	log.Debug(resp)
	assert.NoError(t, err)
	assert.NotEqual(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 0, len(doneSignal))
}

func verifyRequest(t *testing.T, baseURI string, contentType string, path string, body io.Reader, channel *dptest.BasicSink, metricName string, metricValue datapoint.Value) {
	req, err := http.NewRequest("POST", baseURI+path, body)
	assert.NoError(t, err)
	client := &http.Client{}
	doneSignal := make(chan struct{})
	var dpOut *datapoint.Datapoint
	go func() {
		dpOut = channel.Next()
		doneSignal <- struct{}{}
	}()
	if contentType != "" {
		req.Header.Add("Content-Type", contentType)
	}
	log.WithField("req", req).Debug("Doing req")
	resp, err := client.Do(req)
	log.Debug("Done req")
	log.Debug(resp)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	log.Debug("Waiting for signal")
	_ = <-doneSignal
	assert.Equal(t, metricName, dpOut.Metric)
	assert.Equal(t, metricValue, dpOut.Value)
}

func TestSignalfxJsonV1Handler(t *testing.T) {
	listener, channel, baseURI := localSetup(t)
	defer listener.Close()
	// Test no content type
	body := bytes.NewBuffer([]byte(`{"metric":"ametric", "source":"asource", "value" : 3}{}`))
	verifyRequest(t, baseURI, "", "/v1/datapoint", body, channel, "ametric", datapoint.NewIntValue(3))
	// Test w/ json content type
	body = bytes.NewBuffer([]byte(`{"metric":"ametric3", "source":"asource", "value" : 4.3}{}`))
	verifyRequest(t, baseURI, "application/json", "/datapoint", body, channel, "ametric3", datapoint.NewFloatValue(4.3))
}

func TestSignalfxJSONV1Decoder(t *testing.T) {
	decoder := jsonDecoderV1{}
	req := &http.Request{
		Body: ioutil.NopCloser(bytes.NewBuffer([]byte("INVALID_JSON"))),
	}
	ctx := context.Background()
	e := decoder.Read(ctx, req)
	assert.Error(t, e)
}

func oldBodyDp(dp *com_signalfx_metrics_protobuf.DataPoint) io.Reader {
	dpInBytes, _ := proto.Marshal(dp)
	varintBytes := proto.EncodeVarint(uint64(len(dpInBytes)))
	body := bytes.NewBuffer(append(varintBytes, dpInBytes...))
	return body
}

func TestSignalfxProtobufV1Handler(t *testing.T) {
	listener, channel, baseURI := localSetup(t)
	defer listener.Close()
	protoDatapoint := &com_signalfx_metrics_protobuf.DataPoint{
		Source: workarounds.GolangDoesnotAllowPointerToStringLiteral("asource"),
		Metric: workarounds.GolangDoesnotAllowPointerToStringLiteral("ametric4"),
		Value:  &com_signalfx_metrics_protobuf.Datum{IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(2)},
	}
	// Test int body
	body := oldBodyDp(protoDatapoint)
	verifyRequest(t, baseURI, "application/x-protobuf", "/v1/datapoint", body, channel, "ametric4", datapoint.NewIntValue(2))
}

func TestSignalfxProtobufV1Decoder(t *testing.T) {
	defer log.SetLevel(log.GetLevel())
	log.SetLevel(log.DebugLevel)
	typeGetter := metricHandler{
		metricCreationsMap: make(map[string]com_signalfx_metrics_protobuf.MetricType),
	}
	sendTo := dptest.NewBasicSink()
	ctx := context.Background()

	decoder := protobufDecoderV1{
		typeGetter: &typeGetter,
		sink:       sendTo,
	}
	req := &http.Request{
		Body: ioutil.NopCloser(bytes.NewBuffer([]byte("INVALID_PROTOBUF"))),
	}
	e := decoder.Read(ctx, req)
	assert.Error(t, e)

	varintBytes := proto.EncodeVarint(uint64(100))
	req = &http.Request{
		Body: ioutil.NopCloser(bytes.NewBuffer(varintBytes)),
	}
	e = decoder.Read(ctx, req)
	assert.Error(t, e, "Should get error without anything left to read")

	varintBytes = proto.EncodeVarint(uint64(0))
	req = &http.Request{
		Body: ioutil.NopCloser(bytes.NewBuffer(append(varintBytes, []byte("asfasdfsda")...))),
	}
	e = decoder.Read(ctx, req)
	assert.Error(t, e, "Should get error reading len zero")

	varintBytes = proto.EncodeVarint(uint64(200000))
	log.WithField("bytes", varintBytes).Debug("Encoding")
	req = &http.Request{
		Body: ioutil.NopCloser(bytes.NewBuffer(append(varintBytes, []byte("abasdfsadfafdsc")...))),
	}
	e = decoder.Read(ctx, req)
	assert.Equal(t, errProtobufTooLarge, e, "Should get error reading len zero")

	varintBytes = proto.EncodeVarint(uint64(999999999))
	log.WithField("bytes", varintBytes).Debug("Encoding")
	req = &http.Request{
		Body: ioutil.NopCloser(bytes.NewBuffer([]byte{byte(255), byte(147), byte(235), byte(235), byte(235)})),
	}
	e = decoder.Read(ctx, req)
	assert.Equal(t, errInvalidProtobufVarint, e)

	req = &http.Request{
		Body: ioutil.NopCloser(&errorReader{}),
	}
	e = decoder.Read(ctx, req)
	assert.Error(t, e, "Should get error reading if reader fails")

	varintBytes = proto.EncodeVarint(uint64(20))
	req = &http.Request{
		Body: ioutil.NopCloser(bytes.NewBuffer(append(varintBytes, []byte("01234567890123456789")...))),
	}
	e = decoder.Read(ctx, req)
	assert.Contains(t, e.Error(), "proto")

	assert.Equal(t, 0, len(sendTo.PointsChan))
}

func TestSignalfxJSONV2Decoder(t *testing.T) {
	decoder := JsonDecoderV2{}
	req := &http.Request{
		Body: ioutil.NopCloser(bytes.NewBuffer([]byte("INVALID_JSON"))),
	}
	req.ContentLength = -1
	ctx := context.Background()
	e := decoder.Read(ctx, req)
	_ = e.(*json.SyntaxError)
}

func TestSignalfxJsonV2Handler(t *testing.T) {
	listener, channel, baseURI := localSetup(t)
	defer listener.Close()
	body := bytes.NewBuffer([]byte(`{"unused":[], "gauge":[{"metric":"noval", "value":{"a":"b"}}, {"metric":"metrictwo", "value": 3}]}`))
	verifyRequest(t, baseURI, "application/json", "/v2/datapoint", body, channel, "metrictwo", datapoint.NewIntValue(3))
	body = bytes.NewBuffer([]byte(`{"unused":[], "gauge":[{"metric":"noval", "value":{"a":"b"}}, {"metric":"metrictwo", "value": 3}]}`))
	verifyRequest(t, baseURI, "", "/v2/datapoint", body, channel, "metrictwo", datapoint.NewIntValue(3))
}

func TestSignalfxProtoV2Decoder(t *testing.T) {
	decoder := ProtobufDecoderV2{}
	req := &http.Request{
		Body: ioutil.NopCloser(bytes.NewBufferString("")),
	}
	req.ContentLength = -1
	ctx := context.Background()
	assert.Equal(t, errInvalidContentLength, decoder.Read(ctx, req))

	req = &http.Request{
		Body: ioutil.NopCloser(&errorReader{}),
	}
	req.ContentLength = 1
	e := decoder.Read(ctx, req)
	assert.Equal(t, "could not read", e.Error())

	req = &http.Request{
		Body: ioutil.NopCloser(bytes.NewBufferString("INVALID_JSON")),
	}
	req.ContentLength = int64(len("INVALID_JSON"))
	e = decoder.Read(ctx, req)
	assert.Contains(t, e.Error(), "unknown wire type")
}

func TestSignalfxProtoV2Handler(t *testing.T) {
	listener, channel, baseURI := localSetup(t)
	defer listener.Close()

	protoDatapoint := &com_signalfx_metrics_protobuf.DataPoint{
		Source: workarounds.GolangDoesnotAllowPointerToStringLiteral("asource"),
		Metric: workarounds.GolangDoesnotAllowPointerToStringLiteral("ametric4"),
		Value:  &com_signalfx_metrics_protobuf.Datum{IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(2)},
	}
	uploadMsg := &com_signalfx_metrics_protobuf.DataPointUploadMessage{
		Datapoints: []*com_signalfx_metrics_protobuf.DataPoint{protoDatapoint},
	}
	dpInBytes, _ := proto.Marshal(uploadMsg)
	body := bytes.NewBuffer(dpInBytes)
	verifyRequest(t, baseURI, "application/x-protobuf", "/v2/datapoint", body, channel, "ametric4", datapoint.NewIntValue(2))
}

func TestMetricHandler(t *testing.T) {
	handler := metricHandler{
		metricCreationsMap: make(map[string]com_signalfx_metrics_protobuf.MetricType),
	}
	assert.Equal(t, com_signalfx_metrics_protobuf.MetricType_GAUGE, handler.GetMetricTypeFromMap("test"))
	handler.metricCreationsMap["test"] = com_signalfx_metrics_protobuf.MetricType_COUNTER
	assert.Equal(t, com_signalfx_metrics_protobuf.MetricType_COUNTER, handler.GetMetricTypeFromMap("test"))

	// Testing OK write
	rw := httptest.NewRecorder()
	d := []MetricCreationStruct{}
	b, err := json.Marshal(d)
	assert.NoError(t, err)
	w := bytes.NewBuffer(b)
	req := &http.Request{
		Body: ioutil.NopCloser(w),
	}
	handler.ServeHTTP(rw, req)
	assert.Equal(t, "[]", rw.Body.String())
	assert.Equal(t, rw.Code, http.StatusOK)

	// Testing invalid metric write
	rw = httptest.NewRecorder()
	d = []MetricCreationStruct{{MetricType: "Invalid"}}
	b, err = json.Marshal(d)
	assert.NoError(t, err)
	w = bytes.NewBuffer(b)
	req = &http.Request{
		Body: ioutil.NopCloser(w),
	}
	handler.ServeHTTP(rw, req)
	assert.Contains(t, rw.Body.String(), "Invalid metric type")
	assert.Equal(t, rw.Code, http.StatusBadRequest)

	// Testing valid metric write
	rw = httptest.NewRecorder()
	d = []MetricCreationStruct{{MetricType: "COUNTER", MetricName: "ametric"}}
	b, err = json.Marshal(d)
	assert.NoError(t, err)
	w = bytes.NewBuffer(b)
	req = &http.Request{
		Body: ioutil.NopCloser(w),
	}
	handler.ServeHTTP(rw, req)
	assert.Equal(t, rw.Code, http.StatusOK)

	assert.Equal(t, com_signalfx_metrics_protobuf.MetricType_COUNTER, handler.GetMetricTypeFromMap("ametric"))

	rw = httptest.NewRecorder()
	handler.jsonMarshal = func(v interface{}) ([]byte, error) {
		return nil, fmt.Errorf("unable to marshal")
	}
	w = bytes.NewBuffer(b)
	req = &http.Request{
		Body: ioutil.NopCloser(w),
	}
	handler.ServeHTTP(rw, req)
	assert.Equal(t, rw.Code, http.StatusBadRequest)
	assert.Contains(t, rw.Body.String(), "Unable to marshal")
}

func TestStatSize(t *testing.T) {
	listener, _, _ := localSetup(t)
	defer listener.Close()
	assert.Equal(t, 54, len(listener.Stats()))
}

func TestInvalidContentType(t *testing.T) {
	listener, _, baseURI := localSetup(t)
	defer listener.Close()
	// Test no content type
	body := bytes.NewBuffer([]byte(`{"metric":"ametric", "source":"asource", "value" : 3}{}`))
	verifyFailure(t, "GET", baseURI, "application/json", "/datapoint", body)
	verifyFailure(t, "GET", baseURI, "", "/datapoint", body)
	verifyFailure(t, "POST", baseURI, "INVALID_TYPE", "/v1/datapoint", body)
	verifyFailure(t, "POST", baseURI, "INVALID_TYPE", "/datapoint", body)
	verifyFailure(t, "POST", baseURI, "INVALID_TYPE", "/v2/datapoint", body)
	verifyFailure(t, "POST", baseURI, "INVALID_TYPE", "/v1/collectd", body)
	verifyFailure(t, "POST", baseURI, "INVALID_TYPE", "/metric", body)
	verifyFailure(t, "POST", baseURI, "INVALID_TYPE", "/v1/metric", body)
}

type errTest string

func (e errTest) Read(_ context.Context, _ *http.Request) error {
	s := string(e)
	if s == "" {
		return nil
	}
	return fmt.Errorf(s)
}

func TestErrorTrackerHandler(t *testing.T) {
	e := ErrorTrackerHandler{
		reader: errTest(""),
	}
	ctx := context.Background()
	rw := httptest.NewRecorder()
	e.ServeHTTPC(ctx, rw, nil)
	assert.Equal(t, 0, e.TotalErrors)
	assert.Equal(t, `"OK"`, rw.Body.String())
	e.reader = errTest("hi")
	rw = httptest.NewRecorder()
	e.ServeHTTPC(ctx, rw, nil)
	assert.Equal(t, `hi`, rw.Body.String())
	assert.Equal(t, 1, e.TotalErrors)
}

func BenchmarkAtomicInc(b *testing.B) {
	l := int64(0)
	for i := int64(0); i < int64(b.N); i++ {
		atomic.AddInt64(&l, i)
	}
	b.SetBytes(int64(b.N * 8))
}

func BenchmarkRegularInc(b *testing.B) {
	l := int64(0)
	for i := int64(0); i < int64(b.N); i++ {
		l += i
	}
	b.SetBytes(int64(b.N * 8))
}
