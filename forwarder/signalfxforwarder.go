package forwarder

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/golang/glog"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode"
)

var jsonXXXMarshal = json.Marshal
var protoXXXMarshal = proto.Marshal
var jsonXXXUnmarshal = json.Unmarshal
var ioutilXXXReadAll = ioutil.ReadAll

type metricCreationRequest struct {
	toCreate        map[string]com_signalfuse_metrics_protobuf.MetricType
	responseChannel chan (error)
}

type signalfxJSONConnector struct {
	*basicBufferedForwarder
	url                string
	metricCreationChan chan *metricCreationRequest
	connectionTimeout  time.Duration
	defaultAuthToken   string
	tr                 *http.Transport
	sendVersion        int
	client             *http.Client
	userAgent          string
	defaultSource      string
	dimensionSources   []string
	// Map of all metric names to if we've created them with their metric type
	v1MetricLoadedCache      map[string]struct{}
	v1MetricLoadedCacheMutex sync.Mutex
	MetricCreationURL        string
}

var defaultConfigV1 = &config.ForwardTo{
	URL:               workarounds.GolangDoesnotAllowPointerToStringLiteral("https://api.signalfuse.com/v1/datapoint"),
	DefaultSource:     workarounds.GolangDoesnotAllowPointerToStringLiteral(""),
	MetricCreationURL: workarounds.GolangDoesnotAllowPointerToStringLiteral("https://api.signalfuse.com/v1/metric?bulkupdate=true"),
	TimeoutDuration:   workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	BufferSize:        workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(10000)),
	DrainingThreads:   workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(5)),
	Name:              workarounds.GolangDoesnotAllowPointerToStringLiteral("signalfx-forwarder"),
	MaxDrainSize:      workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(1000)),
	SourceDimensions:  workarounds.GolangDoesnotAllowPointerToStringLiteral(""),
	FormatVersion:     workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(1)),
}

var defaultConfigV2 = &config.ForwardTo{
	URL:               workarounds.GolangDoesnotAllowPointerToStringLiteral("https://api.signalfuse.com/v2/datapoint"),
	DefaultSource:     workarounds.GolangDoesnotAllowPointerToStringLiteral(""),
	MetricCreationURL: workarounds.GolangDoesnotAllowPointerToStringLiteral(""), // Not used
	TimeoutDuration:   workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 60),
	BufferSize:        workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(1000000)),
	DrainingThreads:   workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(10)),
	Name:              workarounds.GolangDoesnotAllowPointerToStringLiteral("signalfx-forwarder"),
	MaxDrainSize:      workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(3000)),
	SourceDimensions:  workarounds.GolangDoesnotAllowPointerToStringLiteral(""),
	FormatVersion:     workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(3)),
}

// SignalfxJSONForwarderLoader loads a json forwarder forwarding points from proxy to SignalFx
func SignalfxJSONForwarderLoader(forwardTo *config.ForwardTo) (core.StatKeepingStreamingAPI, error) {
	if forwardTo.FormatVersion == nil || *forwardTo.FormatVersion == 1 {
		structdefaults.FillDefaultFrom(forwardTo, defaultConfigV1)
	} else if *forwardTo.FormatVersion == 2 || *forwardTo.FormatVersion == 3 {
		structdefaults.FillDefaultFrom(forwardTo, defaultConfigV2)
	}
	glog.Infof("Creating signalfx forwarder using final config %s", forwardTo)
	return NewSignalfxJSONForwarer(*forwardTo.URL, *forwardTo.TimeoutDuration, *forwardTo.BufferSize,
		*forwardTo.DefaultAuthToken, *forwardTo.DrainingThreads, *forwardTo.Name, *forwardTo.MetricCreationURL,
		*forwardTo.MaxDrainSize, *forwardTo.DefaultSource, *forwardTo.SourceDimensions, int(*forwardTo.FormatVersion))
}

// NewSignalfxJSONForwarer creates a new JSON forwarder
func NewSignalfxJSONForwarer(url string, timeout time.Duration, bufferSize uint32,
	defaultAuthToken string, drainingThreads uint32, name string, MetricCreationURL string,
	maxDrainSize uint32, defaultSource string, sourceDimensions string, sendVersion int) (core.StatKeepingStreamingAPI, error) {
	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		MaxIdleConnsPerHost:   int(drainingThreads) * 2,
		ResponseHeaderTimeout: timeout,
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, timeout)
		},
	}
	ret := &signalfxJSONConnector{
		basicBufferedForwarder: newBasicBufferedForwarder(bufferSize, maxDrainSize, name, drainingThreads),
		url:              url,
		defaultAuthToken: defaultAuthToken,
		sendVersion:      sendVersion,
		userAgent:        fmt.Sprintf("SignalfxProxy/0.2 (gover %s)", runtime.Version()),
		tr:               tr,
		client: &http.Client{
			Transport: tr,
		},
		connectionTimeout:   timeout,
		v1MetricLoadedCache: map[string]struct{}{},
		MetricCreationURL:   MetricCreationURL,
		defaultSource:       defaultSource,
		metricCreationChan:  make(chan *metricCreationRequest),
		// sf_source is always a dimension that can be a source
		dimensionSources: append([]string{"sf_source"}, strings.Split(sourceDimensions, ",")...),
	}
	ret.start(ret.process)
	go ret.metricCreationLoop()
	return ret, nil
}

func (connector *signalfxJSONConnector) encodePostBodyV2(datapoints []core.Datapoint) ([]byte, string, error) {
	bodyToSend := make(protocoltypes.SignalfxJSONDatapointV2)
	for _, dp := range datapoints {
		bsf := &protocoltypes.BodySendFormatV2{
			Metric:     dp.Metric(),
			Timestamp:  dp.Timestamp().UnixNano() / time.Millisecond.Nanoseconds(),
			Dimensions: dp.Dimensions(),
		}
		f, err := dp.Value().FloatValue()
		if err == nil {
			bsf.Value = f
		} else {
			bsf.Value = dp.Value().WireValue()
		}
		_, ok := bodyToSend[dp.MetricType().String()]
		if !ok {
			bodyToSend[dp.MetricType().String()] = make([]*protocoltypes.BodySendFormatV2, 0)
		}
		bodyToSend[dp.MetricType().String()] = append(bodyToSend[dp.MetricType().String()], bsf)
	}
	jsonBytes, err := jsonXXXMarshal(&bodyToSend)
	glog.V(3).Infof("Posting %s from %s", jsonBytes, bodyToSend)

	// Now we can send datapoints
	return jsonBytes, "application/json", err
}

func (connector *signalfxJSONConnector) encodePostBodyProtobufV2(datapoints []core.Datapoint) ([]byte, string, error) {
	dps := []*com_signalfuse_metrics_protobuf.DataPoint{}
	for _, dp := range datapoints {
		dps = append(dps, connector.coreDatapointToProtobuf(dp))
	}
	msg := &com_signalfuse_metrics_protobuf.DataPointUploadMessage{
		Datapoints: dps,
	}
	protobytes, err := protoXXXMarshal(msg)
	glog.V(3).Infof("Posting %s from %s", protobytes, dps)

	// Now we can send datapoints
	return protobytes, "application/x-protobuf", err
}

func datumForPoint(pv value.DatapointValue) *com_signalfuse_metrics_protobuf.Datum {
	i, err := pv.IntValue()
	if err == nil {
		return &com_signalfuse_metrics_protobuf.Datum{IntValue: &i}
	}
	f, err := pv.FloatValue()
	if err == nil {
		return &com_signalfuse_metrics_protobuf.Datum{DoubleValue: &f}
	}
	s := pv.WireValue()
	return &com_signalfuse_metrics_protobuf.Datum{StrValue: &s}
}

func (connector *signalfxJSONConnector) metricCreationLoop() {
	for {
		glog.V(3).Infof("Waiting for creation request")
		request := <-connector.metricCreationChan
		glog.V(3).Infof("Got creation request: %s", request)
		resp := connector.createMetricsOfType(request.toCreate)
		request.responseChannel <- resp
	}
}

func (connector *signalfxJSONConnector) createMetricsOfType(metricsToCreate map[string]com_signalfuse_metrics_protobuf.MetricType) error {
	if len(metricsToCreate) == 0 {
		return nil
	}
	postBody := []protocoltypes.SignalfxMetricCreationStruct{}
	for metricName, metricType := range metricsToCreate {
		postBody = append(postBody, protocoltypes.SignalfxMetricCreationStruct{
			MetricName: metricName,
			MetricType: metricType.String(),
		})
	}
	jsonBytes, err := jsonXXXMarshal(&postBody)
	if err != nil {
		glog.Warningf("Unable to marshal body: %s", err)
		return err
	}
	glog.V(3).Infof("Posting %s from %s", jsonBytes, postBody)

	req, _ := http.NewRequest("POST", connector.MetricCreationURL, bytes.NewBuffer(jsonBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-SF-TOKEN", connector.defaultAuthToken)
	req.Header.Set("User-Agent", connector.userAgent)

	req.Header.Set("Connection", "Keep-Alive")
	glog.V(3).Infof("Request is %s", req)
	resp, err := connector.client.Do(req)

	if err != nil {
		glog.Warningf("Unable to POST response: %s", err)
		return err
	}

	defer resp.Body.Close()
	respBody, err := ioutilXXXReadAll(resp.Body)
	if err != nil {
		glog.Warningf("Unable to verify response body: %s", err)
		return err
	}
	if resp.StatusCode != 200 {
		glog.Warningf("Metric creation failed: %s", respBody)
		return fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}
	var metricCreationBody []protocoltypes.SignalfxMetricCreationResponse
	err = jsonXXXUnmarshal(respBody, &metricCreationBody)
	if err != nil {
		glog.Warningf("body=(%s), err=(%s)", respBody, err)
		return err
	}
	connector.v1MetricLoadedCacheMutex.Lock()
	defer connector.v1MetricLoadedCacheMutex.Unlock()
	for index, resp := range metricCreationBody {
		metricName := postBody[index].MetricName
		if resp.Code == 0 || resp.Code == 409 {
			connector.v1MetricLoadedCache[metricName] = struct{}{}
		} else {
			glog.Warningf("Unable to create metric %s: %s", metricName, respBody)
		}
	}
	glog.V(3).Infof("Metric creation %s returned %s", jsonBytes, respBody)
	return nil
}

func (connector *signalfxJSONConnector) figureOutReasonableSource(point core.Datapoint) string {
	for _, sourceName := range connector.dimensionSources {
		thisPointSource := point.Dimensions()[sourceName]
		if thisPointSource != "" {
			return thisPointSource
		}
	}
	return connector.defaultSource
}

func (connector *signalfxJSONConnector) requiresSource() bool {
	return connector.sendVersion == 0 || connector.sendVersion == 1
}

func (connector *signalfxJSONConnector) coreDatapointToProtobuf(point core.Datapoint) *com_signalfuse_metrics_protobuf.DataPoint {
	thisPointSource := connector.figureOutReasonableSource(point)
	if thisPointSource == "" && connector.requiresSource() {
		glog.Warningf("unable to figure out a reasonable source for %s, skipping", point)
		return nil
	}
	m := point.Metric()
	ts := point.Timestamp().UnixNano() / time.Millisecond.Nanoseconds()
	relativeTimeDp, ok := point.(core.TimeRelativeDatapoint)
	if ok {
		ts = relativeTimeDp.RelativeTime()
	}
	mt := point.MetricType()
	v := &com_signalfuse_metrics_protobuf.DataPoint{
		Metric:     &m,
		Timestamp:  &ts,
		Value:      datumForPoint(point.Value()),
		MetricType: &mt,
		Dimensions: mapToDimensions(point.Dimensions()),
	}
	if thisPointSource != "" {
		v.Source = &thisPointSource
	}
	return v
}

func mapToDimensions(dimensions map[string]string) []*com_signalfuse_metrics_protobuf.Dimension {
	ret := []*com_signalfuse_metrics_protobuf.Dimension{}
	for k, v := range dimensions {
		// If someone knows a better way to do this, let me know.  I can't just take the &
		// of k and v because their content changes as the range iterates
		copyOfK := filterSignalfxKey(string([]byte(k)))
		copyOfV := (string([]byte(v)))
		ret = append(ret, (&com_signalfuse_metrics_protobuf.Dimension{
			Key:   &copyOfK,
			Value: &copyOfV,
		}))
	}
	return ret
}

func filterSignalfxKey(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsDigit(r) || unicode.IsLetter(r) || r == '_' {
			return r
		}
		return '_'
	}, str)
}

func (connector *signalfxJSONConnector) encodePostBodyV1(datapoints []core.Datapoint) ([]byte, string, error) {
	var msgBody []byte
	metricsToBeCreated := make(map[string]com_signalfuse_metrics_protobuf.MetricType)
	for _, point := range datapoints {
		if point.MetricType() != com_signalfuse_metrics_protobuf.MetricType_GAUGE {
			preCreated := func() bool {
				connector.v1MetricLoadedCacheMutex.Lock()
				defer connector.v1MetricLoadedCacheMutex.Unlock()
				_, ok := connector.v1MetricLoadedCache[point.Metric()]
				return ok
			}()

			if !preCreated {
				metricsToBeCreated[point.Metric()] = point.MetricType()
			}
		}
		v := connector.coreDatapointToProtobuf(point)
		if v == nil {
			continue
		}
		glog.V(3).Infof("Single datapoint to signalfx: %s", v)
		encodedBytes, err := protoXXXMarshal(v)
		if err != nil {
			return nil, "", err
		}
		msgBody = append(msgBody, proto.EncodeVarint(uint64(len(encodedBytes)))...)
		msgBody = append(msgBody, encodedBytes...)
	}
	glog.V(3).Infof("Posting %s len %d", msgBody, len(msgBody))

	if len(metricsToBeCreated) > 0 {
		// Create metrics if we need to
		creationRequest := &metricCreationRequest{
			toCreate:        metricsToBeCreated,
			responseChannel: make(chan error),
		}
		glog.V(3).Infof("Trying to create metrics first")
		connector.metricCreationChan <- creationRequest
		err := <-creationRequest.responseChannel
		if err != nil {
			return nil, "", err
		}
		glog.V(3).Infof("Metric creation OK")
	}
	return msgBody, "application/x-protobuf", nil
}

func (connector *signalfxJSONConnector) GetStats() []core.Datapoint {
	return []core.Datapoint{}
}

func (connector *signalfxJSONConnector) encodePostBody(datapoints []core.Datapoint) ([]byte, string, error) {
	switch connector.sendVersion {
	case 3:
		return connector.encodePostBodyProtobufV2(datapoints)
	case 2:
		return connector.encodePostBodyV2(datapoints)
	default:
		return connector.encodePostBodyV1(datapoints)
	}
}

func (connector *signalfxJSONConnector) process(datapoints []core.Datapoint) error {
	glog.V(2).Infof("Got %d points: %s\n", len(datapoints), datapoints)
	jsonBytes, bodyType, err := connector.encodePostBody(datapoints)

	if err != nil {
		glog.Warningf("Unable to marshal json: %s", err)
		return err
	}
	req, _ := http.NewRequest("POST", connector.url, bytes.NewBuffer(jsonBytes))
	req.Header.Set("Content-Type", bodyType)
	req.Header.Set("X-SF-TOKEN", connector.defaultAuthToken)
	req.Header.Set("User-Agent", connector.userAgent)

	req.Header.Set("Connection", "Keep-Alive")
	glog.V(3).Infof("Request: %s from %s", req, string(jsonBytes))
	resp, err := connector.client.Do(req)

	if err != nil {
		glog.Warningf("Unable to POST request: %s", err)
		return err
	}

	glog.V(3).Infof("Response: %s", resp)
	defer resp.Body.Close()
	respBody, err := ioutilXXXReadAll(resp.Body)
	if err != nil {
		glog.Warningf("Unable to verify response body: %s", err)
		return err
	}
	if resp.StatusCode != 200 {
		glog.Warningf("Metric upload to %s failed: %s", connector.url, respBody)
		return fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}
	var body string
	err = jsonXXXUnmarshal(respBody, &body)
	if err != nil {
		glog.Warningf("body=(%s), err=(%s)", respBody, err)
		return err
	}
	if body != "OK" {
		glog.Warningf("Response not OK: %s", body)
		return fmt.Errorf("Body decode error: %s", body)
	}
	return nil
}
