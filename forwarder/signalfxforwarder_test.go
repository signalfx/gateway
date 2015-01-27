package forwarder

import (
	"fmt"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/a"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/signalfuse/signalfxproxy/listener"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
	"github.com/stretchr/testify/assert"
	//	"time"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"time"

	"code.google.com/p/goprotobuf/proto"
)

var jsonUnmarshalObj a.JsonUnmarshalObj
var ioutilReadAllObj a.IoutilReadAllObj

func init() {
	jsonXXXUnmarshal = jsonUnmarshalObj.Execute
	ioutilXXXReadAll = ioutilReadAllObj.Execute
	log.SetLevel(log.DebugLevel)
}

func TestBodySendFormat(t *testing.T) {
	b := &protocoltypes.BodySendFormatV2{
		Metric: "cpu",
	}
	assert.Contains(t, b.String(), "cpu", "Expect cpu")
}

type metricPanicDatapoint struct {
	core.Datapoint
}

func (vp *metricPanicDatapoint) Metric() string {
	panic("This shouldn't happen!")
}

func TestSignalfxJSONForwarderLoaderDefaults(t *testing.T) {
	forwardTo := config.ForwardTo{
		FormatVersion:    workarounds.GolangDoesnotAllowPointerToUintLiteral(2),
		DefaultAuthToken: workarounds.GolangDoesnotAllowPointerToStringLiteral("AUTH_TOKEN"),
	}
	forwarder, err := SignalfxJSONForwarderLoader(&forwardTo)
	sfForwarder, _ := forwarder.(*signalfxJSONConnector)
	assert.Nil(t, err)
	assert.Equal(t, "https://api.signalfuse.com/v2/datapoint", sfForwarder.url, "URL should change for version 2")
}

func TestMapToDimensions(t *testing.T) {
	r := map[string]string{
		"dim1":     "val1",
		"dim2":     "val2",
		"key:char": "val3",
	}
	res := mapToDimensions(r)
	expect := make(map[string]string)
	for _, d := range res {
		expect[d.GetKey()] = d.GetValue()
	}
	delete(r, "key:char")
	r["key_char"] = "val3"
	assert.Equal(t, r, expect, "Dimensions don't parse right")
	r["invalid_val"] = ""
	res = mapToDimensions(r)
	delete(r, "invalid_val")
	assert.Equal(t, r, expect, "Dimensions don't parse right")
}

func TestFilterSignalfxString(t *testing.T) {
	assert.Equal(t, "hello", filterSignalfxKey("hello"), "Filter not working")
	assert.Equal(t, "_hello_bob1__", filterSignalfxKey(".hello:bob1_&"), "Filter not working")
}

func TestSignalfxJSONForwarderLoader(t *testing.T) {
	// TODO: Break this out into smaller tests
	listenFromSignalfx := config.ListenFrom{}
	listenFromSignalfx.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:0")

	finalDatapointDestination := newBasicBufferedForwarder(100, 1, "", 1)
	l, err := listener.SignalFxListenerLoader(finalDatapointDestination, &listenFromSignalfx)
	defer l.Close()
	assert.Equal(t, nil, err, "Expect no error")

	port := getListenerPort(l)

	forwardTo := config.ForwardTo{
		URL:               workarounds.GolangDoesnotAllowPointerToStringLiteral(fmt.Sprintf("http://0.0.0.0:%d/v1/datapoint", port)),
		TimeoutDuration:   workarounds.GolangDoesnotAllowPointerToDurationLiteral(time.Second * 1),
		MetricCreationURL: workarounds.GolangDoesnotAllowPointerToStringLiteral(fmt.Sprintf("http://0.0.0.0:%d/v1/metric", port)),
		DefaultAuthToken:  workarounds.GolangDoesnotAllowPointerToStringLiteral("AUTH_TOKEN"),
		DefaultSource:     workarounds.GolangDoesnotAllowPointerToStringLiteral("proxy-source"),
		SourceDimensions:  workarounds.GolangDoesnotAllowPointerToStringLiteral("username,ignored,hostname"),
	}

	forwarder, err := SignalfxJSONForwarderLoader(&forwardTo)
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, "signalfx-forwarder", forwarder.Name(), "Expect no error")
	assert.Equal(t, 0, len(forwarder.GetStats()), "Expect no stats")

	sfForwarder, _ := forwarder.(*signalfxJSONConnector)

	timeToSend := time.Now().Round(time.Second)
	dpSent := core.NewAbsoluteTimeDatapoint("metric", map[string]string{}, value.NewIntWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, timeToSend)
	forwarder.DatapointsChannel() <- dpSent
	dpRecieved := <-finalDatapointDestination.datapointsChannel
	i, _ := dpRecieved.Value().IntValue()
	assert.Equal(t, int64(2), i, "Expect 2 back")
	assert.Equal(t, "proxy-source", dpRecieved.Dimensions()["sf_source"], "Expect ahost back")

	timeToSend = time.Now().Round(time.Second)
	dpSent = core.NewAbsoluteTimeDatapoint("metric", map[string]string{"cpusize": "big", "hostname": "ahost"}, value.NewIntWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, timeToSend)
	forwarder.DatapointsChannel() <- dpSent
	dpRecieved = <-finalDatapointDestination.datapointsChannel
	i, _ = dpRecieved.Value().IntValue()
	assert.Equal(t, int64(2), i, "Expect 2 back")
	assert.Equal(t, "ahost", dpRecieved.Dimensions()["sf_source"], "Expect ahost back")

	dpSent = core.NewAbsoluteTimeDatapoint("metric", map[string]string{}, value.NewFloatWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, timeToSend)
	forwarder.DatapointsChannel() <- dpSent
	dpRecieved = <-finalDatapointDestination.datapointsChannel
	f, _ := dpRecieved.Value().FloatValue()
	assert.Equal(t, 2.0, f, "Expect 2 back")

	dpStr := core.NewAbsoluteTimeDatapoint("metric", map[string]string{}, value.NewStrWire("astr"), com_signalfuse_metrics_protobuf.MetricType_GAUGE, timeToSend)
	forwarder.DatapointsChannel() <- dpStr
	dpRecieved = <-finalDatapointDestination.datapointsChannel
	assert.Equal(t, "astr", dpRecieved.Value().WireValue(), "Expect 2 back")

	// No source should mean we don't ask for the metric
	sfForwarder.defaultSource = ""
	dp := &metricPanicDatapoint{dpSent}
	err = sfForwarder.process([]core.Datapoint{dp})
	sfForwarder.defaultSource = "proxy"
	assert.Equal(t, nil, err, "Expect no error")

	dpSent = core.NewAbsoluteTimeDatapoint("metric", map[string]string{}, value.NewFloatWire(2.0), com_signalfuse_metrics_protobuf.MetricType_COUNTER, timeToSend)
	forwarder.DatapointsChannel() <- dpSent
	dpRecieved = <-finalDatapointDestination.datapointsChannel
	f, _ = dpRecieved.Value().FloatValue()
	assert.Equal(t, 2.0, f, "Expect 2 back")
	assert.Equal(t, com_signalfuse_metrics_protobuf.MetricType_COUNTER, dpRecieved.MetricType(), "Expect 2 back")

	dpSent = core.NewRelativeTimeDatapoint("metric", map[string]string{}, value.NewFloatWire(2.0), com_signalfuse_metrics_protobuf.MetricType_COUNTER, -1)
	forwarder.DatapointsChannel() <- dpSent
	dpRecieved = <-finalDatapointDestination.datapointsChannel
	ts, _ := dpRecieved.(core.TimeRelativeDatapoint)
	assert.Equal(t, int64(-1), ts.RelativeTime(), "Expect -1 time ago")

	log.WithField("chanlen", len(finalDatapointDestination.datapointsChannel)).Info("Starting failing test")
	dpSent = core.NewRelativeTimeDatapoint("metricnowacounter", map[string]string{"sf_source": "asource"}, value.NewFloatWire(2.0), com_signalfuse_metrics_protobuf.MetricType_COUNTER, -1)
	forwarder.DatapointsChannel() <- dpSent
	dpRecieved = <-finalDatapointDestination.datapointsChannel
	_, ok := sfForwarder.v1MetricLoadedCache["metricnowacounter"]
	assert.Equal(t, true, ok, "Expected asource")
	assert.Equal(t, "asource", dpRecieved.Dimensions()["sf_source"], "Expected asource for %s", dpRecieved)

	sfForwarder.MetricCreationURL = "http://0.0.0.0:21/asfd" // invalid
	dpSent = core.NewRelativeTimeDatapoint("anotermetric", map[string]string{}, value.NewFloatWire(2.0), com_signalfuse_metrics_protobuf.MetricType_COUNTER, -1)
	sfForwarder.process([]core.Datapoint{dpSent})
	assert.Equal(t, 0, len(finalDatapointDestination.datapointsChannel), "Expect no metrics")
	sfForwarder.MetricCreationURL = fmt.Sprintf("http://0.0.0.0:%d/v1/metric", port)

	err = sfForwarder.createMetricsOfType(map[string]com_signalfuse_metrics_protobuf.MetricType{})
	assert.Equal(t, nil, err, "Expected no error making no metrics")

	jsonXXXMarshal = func(interface{}) ([]byte, error) { return nil, errors.New("json marshal issue") }
	err = sfForwarder.createMetricsOfType(map[string]com_signalfuse_metrics_protobuf.MetricType{"m": com_signalfuse_metrics_protobuf.MetricType_COUNTER})
	assert.NotEqual(t, nil, err, "Expected no error making no metrics")
	jsonXXXMarshal = json.Marshal

	func() {
		ioutilReadAllObj.UseFunction(func(r io.Reader) ([]byte, error) { return nil, errors.New("ioutil") })
		defer ioutilReadAllObj.Reset()
		err = sfForwarder.createMetricsOfType(map[string]com_signalfuse_metrics_protobuf.MetricType{"m": com_signalfuse_metrics_protobuf.MetricType_COUNTER})
		assert.Contains(t, err.Error(), "ioutil", "Expected ioutil issue")
	}()

	sfForwarder.MetricCreationURL = fmt.Sprintf("http://0.0.0.0:%d/vmetric", port)
	err = sfForwarder.createMetricsOfType(map[string]com_signalfuse_metrics_protobuf.MetricType{"m": com_signalfuse_metrics_protobuf.MetricType_COUNTER})
	assert.Contains(t, err.Error(), "invalid status code", "Expected status code 404")
	sfForwarder.MetricCreationURL = fmt.Sprintf("http://0.0.0.0:%d/v1/metric", port)

	func() {
		ioutilReadAllObj.UseFunction(func(r io.Reader) ([]byte, error) { return []byte("InvalidJson"), nil })
		defer ioutilReadAllObj.Reset()
		err = sfForwarder.createMetricsOfType(map[string]com_signalfuse_metrics_protobuf.MetricType{"m": com_signalfuse_metrics_protobuf.MetricType_COUNTER})
		assert.Contains(t, err.Error(), "invalid character", "Expected ioutil issue")
	}()

	func() {
		ioutilReadAllObj.UseFunction(func(r io.Reader) ([]byte, error) { return []byte("InvalidJson"), nil })
		defer ioutilReadAllObj.Reset()
		err = sfForwarder.createMetricsOfType(map[string]com_signalfuse_metrics_protobuf.MetricType{"m": com_signalfuse_metrics_protobuf.MetricType_COUNTER})
		assert.Contains(t, err.Error(), "invalid character", "Expected ioutil issue")
	}()

	func() {
		ioutilReadAllObj.UseFunction(func(r io.Reader) ([]byte, error) { return []byte(`[{"code":203}]`), nil })
		defer ioutilReadAllObj.Reset()
		err = sfForwarder.createMetricsOfType(map[string]com_signalfuse_metrics_protobuf.MetricType{"wontexist": com_signalfuse_metrics_protobuf.MetricType_COUNTER})
		assert.Equal(t, nil, err, "Expected no error making no metrics")
		_, ok = sfForwarder.v1MetricLoadedCache["wontexist"]
		assert.Equal(t, false, ok, "Should not make")
	}()

	protoXXXMarshal = func(r proto.Message) ([]byte, error) { return nil, errors.New("proto encode error") }
	_, _, err = sfForwarder.encodePostBodyV1([]core.Datapoint{dpSent})
	assert.Equal(t, "proto encode error", err.Error(), "Expected error encoding protobufs")
	protoXXXMarshal = proto.Marshal

	sfForwarder.sendVersion = 2
	_, _, err = sfForwarder.encodePostBody([]core.Datapoint{dpSent, dpStr})
	sfForwarder.sendVersion = 1
	assert.Equal(t, nil, err, "Expected no error making metrics")

	sfForwarder.sendVersion = 3
	_, _, err = sfForwarder.encodePostBody([]core.Datapoint{dpSent, dpStr})
	sfForwarder.sendVersion = 1
	assert.Equal(t, nil, err, "Expected no error making metrics")

	prevURL := sfForwarder.url
	sfForwarder.url = "http://0.0.0.0:12333/vvv/s"
	err = sfForwarder.process([]core.Datapoint{dpSent})
	assert.Contains(t, err.Error(), "connection refused", "Expected error posting points")
	sfForwarder.url = prevURL

	sfForwarder.url = fmt.Sprintf("http://0.0.0.0:%d/v1/metric", port)
	err = sfForwarder.process([]core.Datapoint{dpSent})
	assert.Contains(t, err.Error(), "invalid status code", "Expected error posting points to metric creation url")
	sfForwarder.url = prevURL

	ioutilXXXReadAll = func(r io.Reader) ([]byte, error) { return nil, errors.New("ioutil") }
	err = sfForwarder.process([]core.Datapoint{dpSent})
	assert.Equal(t, "ioutil", err.Error(), "Expected ioutil decoding response")
	ioutilXXXReadAll = ioutil.ReadAll

	func() {
		jsonUnmarshalObj.UseFunction(func([]byte, interface{}) error { return errors.New("jsonUnmarshalError") })
		defer jsonUnmarshalObj.Reset()
		err = sfForwarder.process([]core.Datapoint{dpSent})
		assert.Equal(t, "jsonUnmarshalError", err.Error(), "Expected ioutil decoding response")
	}()

	ioutilXXXReadAll = func(r io.Reader) ([]byte, error) { return []byte(`"invalidbody"`), nil }
	err = sfForwarder.process([]core.Datapoint{dpSent})
	assert.Contains(t, err.Error(), "Body decode error", "Expected body decoding error")
	ioutilXXXReadAll = ioutil.ReadAll
}
