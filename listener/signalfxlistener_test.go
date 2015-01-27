package listener

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"

	"code.google.com/p/goprotobuf/proto"
	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/stretchr/testify/assert"
)

func TestInvalidSignalfxJSONForwarderLoader(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:999999"),
	}
	sendTo := &basicDatapointStreamingAPI{}
	_, err := SignalFxListenerLoader(sendTo, listenFrom)
	assert.NotEqual(t, nil, err, "Should get an error making")
}

type myReaderType struct{}

func (reader *myReaderType) Read(b []byte) (int, error) {
	return 0, errors.New("can not read")
}

func TestReadFully(t *testing.T) {
	myReader := &myReaderType{}
	reader := bufio.NewReader(myReader)
	_, err := fullyReadFromBuffer(reader, 10)
	assert.NotEqual(t, nil, err, "Should get an error making")

	_, err = fullyReadFromBuffer(bufio.NewReader(bytes.NewBuffer([]byte("abcde"))), 7)
	assert.NotEqual(t, nil, err, "Should get an error making (EOF or something)")

	result, err := fullyReadFromBuffer(bufio.NewReader(bytes.NewBuffer([]byte("abcdefg"))), 5)
	assert.Equal(t, nil, err, "Should not get an error")
	assert.Equal(t, "abcde", string(result), "Expect my result back")
}

type errorReader struct{}

func (errorReader *errorReader) Read([]byte) (int, error) {
	return 0, errors.New("Could not read")
}

func TestProtobufDecoding(t *testing.T) {
	DatapointStreamingAPI := &basicDatapointStreamingAPI{
		channel: make(chan core.Datapoint),
	}
	listenerServer := &listenerServer{
		metricCreationsMap:      make(map[string]com_signalfuse_metrics_protobuf.MetricType),
		metricCreationsMapMutex: sync.Mutex{},
		datapointStreamingAPI:   DatapointStreamingAPI,
	}

	protoDatapoint := &com_signalfuse_metrics_protobuf.DataPoint{
		Source: workarounds.GolangDoesnotAllowPointerToStringLiteral("asource"),
		Metric: workarounds.GolangDoesnotAllowPointerToStringLiteral("ametric"),
		Value:  &com_signalfuse_metrics_protobuf.Datum{IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(2)},
	}

	var dpOut core.Datapoint
	go func() {
		dpOut = <-DatapointStreamingAPI.channel
	}()
	dpInBytes, _ := proto.Marshal(protoDatapoint)
	varintBytes := proto.EncodeVarint(uint64(len(dpInBytes)))
	body := bytes.NewBuffer(append(varintBytes, dpInBytes...))
	log.WithField("len", body.Len()).Info("Got body to post")
	assert.Equal(t, nil,
		listenerServer.protobufDecoding(body),
		"Should not get error reading")

	assert.NotEqual(t, nil,
		listenerServer.protobufDecoding(&errorReader{}),
		"Should not get error reading")

	log.Info("Stubbing function")
	protoXXXDecodeVarint = func([]byte) (uint64, int) {
		return 0, 0
	}
	varintBytes = proto.EncodeVarint(uint64(len(dpInBytes)))
	body = bytes.NewBuffer(append(varintBytes, dpInBytes...))
	log.WithField("len", body.Len()).Info("Got body to post")
	assert.NotEqual(t, nil,
		listenerServer.protobufDecoding(body),
		"Should get error decoding protobuf")
	protoXXXDecodeVarint = proto.DecodeVarint

	dpInBytes, _ = proto.Marshal(protoDatapoint)
	varintBytes = proto.EncodeVarint(uint64(len(dpInBytes)))
	body = bytes.NewBuffer(append(varintBytes, dpInBytes[0:5]...))
	log.WithField("len", body.Len()).Info("Short body size")
	assert.NotEqual(t, nil,
		listenerServer.protobufDecoding(body),
		"Should get error reading shorted protobuf")

	protoXXXDecodeVarint = func([]byte) (uint64, int) {
		return 123456, 3
	}
	varintBytes = proto.EncodeVarint(uint64(len(dpInBytes)))
	body = bytes.NewBuffer(append(varintBytes, make([]byte, len(dpInBytes))...))
	log.WithField("len", body.Len()).Info("Got body to post")
	assert.NotEqual(t, nil,
		listenerServer.protobufDecoding(body),
		"Should get error decoding protobuf")
	protoXXXDecodeVarint = proto.DecodeVarint

	varintBytes = proto.EncodeVarint(uint64(len(dpInBytes)))
	body = bytes.NewBuffer(append(varintBytes, make([]byte, len(dpInBytes))...))
	log.WithField("len", body.Len()).Info("Got body to post")
	assert.NotEqual(t, nil,
		listenerServer.protobufDecoding(body),
		"Should get error decoding invalid protobuf")
}

func TestGetMetricTypeFromMap(t *testing.T) {
	metricCreationsMap := make(map[string]com_signalfuse_metrics_protobuf.MetricType)
	metricCreationsMapMutex := sync.Mutex{}
	metricCreationsMap["countername"] = com_signalfuse_metrics_protobuf.MetricType_COUNTER
	listenerServer := &listenerServer{
		metricCreationsMap:      metricCreationsMap,
		metricCreationsMapMutex: metricCreationsMapMutex,
	}
	assert.Equal(t, com_signalfuse_metrics_protobuf.MetricType_COUNTER,
		listenerServer.getMetricTypeFromMap("countername"),
		"Should get back the counter")
	assert.Equal(t, com_signalfuse_metrics_protobuf.MetricType_GAUGE,
		listenerServer.getMetricTypeFromMap("unknown"),
		"Should get the default")
}

func TestSignalfxJSONForwarderInvalidJSONEngine(t *testing.T) {
	sendTo := &basicDatapointStreamingAPI{
		channel: make(chan core.Datapoint),
	}
	listenFrom := &config.ListenFrom{
		JSONEngine: workarounds.GolangDoesnotAllowPointerToStringLiteral("unknown"),
	}
	_, err := SignalFxListenerLoader(sendTo, listenFrom)
	assert.Error(t, err)
}

func TestSignalfxJSONForwarderLoader(t *testing.T) {
	sendTo := &basicDatapointStreamingAPI{
		channel: make(chan core.Datapoint),
	}
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12349"),
	}

	listener, err := SignalFxListenerLoader(sendTo, listenFrom)
	assert.Equal(t, nil, err, "Should not get an error making")
	assert.Equal(t, 16, len(listener.GetStats()), "Should have no stats")

	defer listener.Close()

	req, _ := http.NewRequest("POST", "http://0.0.0.0:12349/v1/datapoint", bytes.NewBuffer([]byte(`{"metric":"ametric", "source":"asource", "value" : 3}{}`)))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	var dp core.Datapoint
	gotPointChan := make(chan bool)
	go func() {
		dp = <-sendTo.channel
		gotPointChan <- true
	}()
	resp, err := client.Do(req)
	_ = <-gotPointChan

	assert.Equal(t, nil, err, "Should not get an error making request")
	assert.Equal(t, resp.StatusCode, 200, "Request should work")
	assert.Equal(t, "ametric", dp.Metric(), "Should get metric back!")
	assert.Equal(t, "asource", dp.Dimensions()["sf_source"], "Should get metric back!")

	req, _ = http.NewRequest(
		"POST",
		"http://0.0.0.0:12349/v2/datapoint",
		bytes.NewBuffer([]byte(`{"unused":[], "gauge":[{"metric":"noval", "value":{"a":"b"}}, {"metric":"metrictwo", "value": 3}]}`)),
	)
	req.Header.Set("Content-Type", "application/json")
	gotPointChan = make(chan bool)
	go func() {
		dp = <-sendTo.channel
		gotPointChan <- true
	}()
	resp, err = client.Do(req)
	_ = <-gotPointChan
	assert.Equal(t, resp.StatusCode, http.StatusOK, "Request should work")
	assert.Equal(t, nil, err, "Should not get an error making request")

	assert.Equal(t, nil, err, "Should not get an error making request")
	assert.Equal(t, resp.StatusCode, 200, "Request should work")
	assert.Equal(t, "metrictwo", dp.Metric(), "Should get metric back!")
	assert.Equal(t, 0, len(dp.Dimensions()), "Should get metric back!")

	req, _ = http.NewRequest(
		"POST",
		"http://0.0.0.0:12349/v1/collectd",
		bytes.NewBuffer([]byte(testCollectdBody)),
	)
	req.Header.Set("Content-Type", "application/json")
	gotPointChan = make(chan bool)
	go func() {
		for i := 0; i < 5; i++ {
			dp = <-sendTo.channel
		}
		gotPointChan <- true
	}()
	resp, err = client.Do(req)
	_ = <-gotPointChan
	assert.Equal(t, resp.StatusCode, http.StatusOK, "Request should work")
	assert.Equal(t, nil, err, "Should not get an error making request")

	assert.Equal(t, nil, err, "Should not get an error making request")
	assert.Equal(t, resp.StatusCode, 200, "Request should work")
	assert.Equal(t, "df_complex.free", dp.Metric(), "Should get metric back!")
	assert.Equal(t, 4, len(dp.Dimensions()), "Should get metric back!")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/datapoint", bytes.NewBuffer([]byte(`INVALIDJSON`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	assert.Equal(t, resp.StatusCode, 400, "Request should not work")
	assert.Equal(t, nil, err, "Should not get an error making request")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/datapoint", bytes.NewBuffer([]byte(`{}`)))
	req.Header.Set("Content-Type", "UNKNOWNTYPE")
	resp, err = client.Do(req)
	assert.Equal(t, resp.StatusCode, http.StatusBadRequest, "Request should not work")
	assert.Equal(t, nil, err, "Should not get an error making request")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/metric", bytes.NewBuffer([]byte(`[{"sf_metric": "nowacounter", "sf_metricType":"COUNTER"}]`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	assert.Equal(t, resp.StatusCode, 200, "Request should work")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/metric", bytes.NewBuffer([]byte(`[{"sf_metric": "invalid", "sf_metricType":"INVALIDTYPE"}]`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	assert.Equal(t, resp.StatusCode, http.StatusBadRequest, "Request should not work: invalid type")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/metric", bytes.NewBuffer([]byte(`INVALIDJSONFORMETRIC`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	assert.Equal(t, resp.StatusCode, http.StatusBadRequest, "Request should not work: invalid type")

	protoDatapoint := &com_signalfuse_metrics_protobuf.DataPoint{
		Source: workarounds.GolangDoesnotAllowPointerToStringLiteral("asource"),
		Metric: workarounds.GolangDoesnotAllowPointerToStringLiteral("ametric"),
		Value:  &com_signalfuse_metrics_protobuf.Datum{IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(2)},
	}
	dpInBytes, _ := proto.Marshal(protoDatapoint)
	varintBytes := proto.EncodeVarint(uint64(len(dpInBytes)))
	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/datapoint", bytes.NewBuffer(append(varintBytes, dpInBytes...)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	gotPointChan = make(chan bool)
	go func() {
		dp = <-sendTo.channel
		gotPointChan <- true
	}()
	resp, err = client.Do(req)
	_ = <-gotPointChan
	assert.Equal(t, resp.StatusCode, 200, "Request should work")
	assert.Equal(t, "asource", dp.Dimensions()["sf_source"], "Expect source back")
	assert.Equal(t, "2", dp.Value().WireValue(), "Expect 2 back")

	uploadMsg := &com_signalfuse_metrics_protobuf.DataPointUploadMessage{
		Datapoints: []*com_signalfuse_metrics_protobuf.DataPoint{protoDatapoint},
	}
	dpInBytes, _ = proto.Marshal(uploadMsg)
	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v2/datapoint", bytes.NewBuffer(dpInBytes))
	req.Header.Set("Content-Type", "application/x-protobuf")
	go func() {
		dp = <-sendTo.channel
		gotPointChan <- true
	}()
	resp, err = client.Do(req)
	_ = <-gotPointChan
	assert.Equal(t, resp.StatusCode, 200, "Request should work")
	assert.Equal(t, "asource", dp.Dimensions()["sf_source"], "Expect source back")
	assert.Equal(t, "2", dp.Value().WireValue(), "Expect 2 back")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v2/datapoint", bytes.NewBuffer([]byte(`invalid`)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err = client.Do(req)
	assert.Equal(t, resp.StatusCode, http.StatusBadRequest, "Request should not work: length issue")

	reqObj := &http.Request{
		ContentLength: -1,
	}
	listenerServer := &listenerServer{
		datapointStreamingAPI: sendTo,
	}
	assert.NotNil(t, listenerServer.protobufDecoderFunctionV2()(reqObj))
	reqObj = &http.Request{
		ContentLength: 20,
		Body:          ioutil.NopCloser(strings.NewReader("abcd")),
	}
	assert.NotNil(t, listenerServer.protobufDecoderFunctionV2()(reqObj))

	jsonXXXMarshal = func(interface{}) ([]byte, error) {
		return nil, errors.New("Unable to marshal json")
	}
	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/metric", bytes.NewBuffer([]byte(`[{"sf_metric": "nowacounter", "sf_metricType":"COUNTER"}]`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	assert.Equal(t, resp.StatusCode, http.StatusBadRequest, "Request should not work: json issue")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v2/datapoint", bytes.NewBuffer([]byte(`[{"sf_metric": "nowacounter", "sf_metricType":"COUNTER"}]`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	assert.Equal(t, resp.StatusCode, http.StatusBadRequest, "Request should not work: json issue")
	jsonXXXMarshal = json.Marshal
}
