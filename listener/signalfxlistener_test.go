package listener

import (
	"bufio"
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/json"
	"errors"
	"github.com/cep21/gohelpers/a"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/golang/glog"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"net/http"
	"sync"
	"testing"
)

func TestInvalidSignalfxJSONForwarderLoader(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:999999"),
	}
	sendTo := &basicDatapointStreamingAPI{}
	_, err := SignalFxListenerLoader(sendTo, listenFrom)
	a.ExpectNotEquals(t, nil, err, "Should get an error making")
}

type myReaderType struct{}

func (reader *myReaderType) Read(b []byte) (int, error) {
	return 0, errors.New("can not read")
}

func TestReadFully(t *testing.T) {
	myReader := &myReaderType{}
	reader := bufio.NewReader(myReader)
	_, err := fullyReadFromBuffer(reader, 10)
	a.ExpectNotEquals(t, nil, err, "Should get an error making")

	_, err = fullyReadFromBuffer(bufio.NewReader(bytes.NewBuffer([]byte("abcde"))), 7)
	a.ExpectNotEquals(t, nil, err, "Should get an error making (EOF or something)")

	result, err := fullyReadFromBuffer(bufio.NewReader(bytes.NewBuffer([]byte("abcdefg"))), 5)
	a.ExpectEquals(t, nil, err, "Should not get an error")
	a.ExpectEquals(t, "abcde", string(result), "Expect my result back")
}

type errorReader struct{}

func (errorReader *errorReader) Read([]byte) (int, error) {
	return 0, errors.New("Could not read")
}

func TestProtobufDecoding(t *testing.T) {
	metricCreationsMap := make(map[string]com_signalfuse_metrics_protobuf.MetricType)
	metricCreationsMapMutex := &sync.Mutex{}
	DatapointStreamingAPI := &basicDatapointStreamingAPI{
		channel: make(chan core.Datapoint),
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
	glog.Infof("Body size: %d", body.Len())
	a.ExpectEquals(t, nil,
		protobufDecoding(body, metricCreationsMap, metricCreationsMapMutex, DatapointStreamingAPI),
		"Should not get error reading")

	a.ExpectNotEquals(t, nil,
		protobufDecoding(&errorReader{}, metricCreationsMap, metricCreationsMapMutex, DatapointStreamingAPI),
		"Should not get error reading")

	glog.Infof("Stubbing function")
	protoXXXDecodeVarint = func([]byte) (uint64, int) {
		return 0, 0
	}
	varintBytes = proto.EncodeVarint(uint64(len(dpInBytes)))
	body = bytes.NewBuffer(append(varintBytes, dpInBytes...))
	glog.Infof("Body size: %d", body.Len())
	a.ExpectNotEquals(t, nil,
		protobufDecoding(body, metricCreationsMap, metricCreationsMapMutex, DatapointStreamingAPI),
		"Should get error decoding protobuf")
	protoXXXDecodeVarint = proto.DecodeVarint

	dpInBytes, _ = proto.Marshal(protoDatapoint)
	varintBytes = proto.EncodeVarint(uint64(len(dpInBytes)))
	body = bytes.NewBuffer(append(varintBytes, dpInBytes[0:5]...))
	glog.Infof("Short body size: %d", body.Len())
	a.ExpectNotEquals(t, nil,
		protobufDecoding(body, metricCreationsMap, metricCreationsMapMutex, DatapointStreamingAPI),
		"Should get error reading shorted protobuf")

	protoXXXDecodeVarint = func([]byte) (uint64, int) {
		return 123456, 3
	}
	varintBytes = proto.EncodeVarint(uint64(len(dpInBytes)))
	body = bytes.NewBuffer(append(varintBytes, make([]byte, len(dpInBytes))...))
	glog.Infof("Body size: %d", body.Len())
	a.ExpectNotEquals(t, nil,
		protobufDecoding(body, metricCreationsMap, metricCreationsMapMutex, DatapointStreamingAPI),
		"Should get error decoding protobuf")
	protoXXXDecodeVarint = proto.DecodeVarint

	varintBytes = proto.EncodeVarint(uint64(len(dpInBytes)))
	body = bytes.NewBuffer(append(varintBytes, make([]byte, len(dpInBytes))...))
	glog.Infof("Body size: %d", body.Len())
	a.ExpectNotEquals(t, nil,
		protobufDecoding(body, metricCreationsMap, metricCreationsMapMutex, DatapointStreamingAPI),
		"Should get error decoding invalid protobuf")
}

func TestGetMetricTypeFromMap(t *testing.T) {
	metricCreationsMap := make(map[string]com_signalfuse_metrics_protobuf.MetricType)
	metricCreationsMapMutex := &sync.Mutex{}
	metricCreationsMap["countername"] = com_signalfuse_metrics_protobuf.MetricType_COUNTER
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_COUNTER,
		getMetricTypeFromMap(metricCreationsMap, metricCreationsMapMutex, "countername"),
		"Should get back the counter")
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_GAUGE,
		getMetricTypeFromMap(metricCreationsMap, metricCreationsMapMutex, "unknown"),
		"Should get the default")
}

func TestSignalfxJSONForwarderLoader(t *testing.T) {
	sendTo := &basicDatapointStreamingAPI{
		channel: make(chan core.Datapoint),
	}
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12349"),
	}

	listener, err := SignalFxListenerLoader(sendTo, listenFrom)
	a.ExpectEquals(t, nil, err, "Should not get an error making")
	a.ExpectEquals(t, 0, len(listener.GetStats()), "Should have no stats")

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

	a.ExpectEquals(t, nil, err, "Should not get an error making request")
	a.ExpectEquals(t, resp.StatusCode, 200, "Request should work")
	a.ExpectEquals(t, "ametric", dp.Metric(), "Should get metric back!")
	a.ExpectEquals(t, "asource", dp.Dimensions()["sf_source"], "Should get metric back!")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/datapoint", bytes.NewBuffer([]byte(`INVALIDJSON`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	a.ExpectEquals(t, resp.StatusCode, 400, "Request should not work")
	a.ExpectEquals(t, nil, err, "Should not get an error making request")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/datapoint", bytes.NewBuffer([]byte(`{}`)))
	req.Header.Set("Content-Type", "UNKNOWNTYPE")
	resp, err = client.Do(req)
	a.ExpectEquals(t, resp.StatusCode, http.StatusBadRequest, "Request should not work")
	a.ExpectEquals(t, nil, err, "Should not get an error making request")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/metric", bytes.NewBuffer([]byte(`[{"sf_metric": "nowacounter", "sf_metricType":"COUNTER"}]`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	a.ExpectEquals(t, resp.StatusCode, 200, "Request should work")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/metric", bytes.NewBuffer([]byte(`[{"sf_metric": "invalid", "sf_metricType":"INVALIDTYPE"}]`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	a.ExpectEquals(t, resp.StatusCode, http.StatusBadRequest, "Request should not work: invalid type")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/metric", bytes.NewBuffer([]byte(`INVALIDJSONFORMETRIC`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	a.ExpectEquals(t, resp.StatusCode, http.StatusBadRequest, "Request should not work: invalid type")

	protoDatapoint := &com_signalfuse_metrics_protobuf.DataPoint{
		Source: workarounds.GolangDoesnotAllowPointerToStringLiteral("asource"),
		Metric: workarounds.GolangDoesnotAllowPointerToStringLiteral("ametric"),
		Value:  &com_signalfuse_metrics_protobuf.Datum{IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(2)},
	}
	dpInBytes, _ := proto.Marshal(protoDatapoint)
	varintBytes := proto.EncodeVarint(uint64(len(dpInBytes)))
	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/datapoint", bytes.NewBuffer(append(varintBytes, dpInBytes...)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	go func() {
		dp = <-sendTo.channel
		gotPointChan <- true
	}()
	resp, err = client.Do(req)
	a.ExpectEquals(t, resp.StatusCode, 200, "Request should work")
	a.ExpectEquals(t, "asource", dp.Dimensions()["sf_source"], "Expect source back")
	a.ExpectEquals(t, "2", dp.Value().WireValue(), "Expect 2 back")

	jsonXXXMarshal = func(interface{}) ([]byte, error) {
		return nil, errors.New("Unable to marshal json")
	}
	req, _ = http.NewRequest("POST", "http://0.0.0.0:12349/v1/metric", bytes.NewBuffer([]byte(`[{"sf_metric": "nowacounter", "sf_metricType":"COUNTER"}]`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	a.ExpectEquals(t, resp.StatusCode, http.StatusBadRequest, "Request should not work: json issue")
	jsonXXXMarshal = json.Marshal
}
