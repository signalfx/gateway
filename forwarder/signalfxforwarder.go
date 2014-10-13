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
	"net/http"
	"runtime"
	"sync"
	"time"
)

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
	// Map of all metric names to if we've created them with their metric type
	v1MetricLoadedCache      map[string]struct{}
	v1MetricLoadedCacheMutex sync.Mutex
	MetricCreationURL        string
}

// ValueToSend are values are sent from the proxy to a reciever for the datapoint
type ValueToSend interface {
}

// BodySendFormat is the JSON format signalfx datapoints are expected to be in
type BodySendFormat struct {
	Metric     string            `json:"metric"`
	Timestamp  int64             `json:"timestamp"`
	Value      ValueToSend       `json:"value"`
	Dimensions map[string]string `json:"dimensions"`
}

func (bodySendFormat *BodySendFormat) String() string {
	return fmt.Sprintf("DP[metric=%s|time=%d|val=%s|dimensions=%s]", bodySendFormat.Metric, bodySendFormat.Timestamp, bodySendFormat.Value, bodySendFormat.Dimensions)
}

var defaultConfig = &config.ForwardTo{
	URL:               workarounds.GolangDoesnotAllowPointerToStringLiteral("https://api.signalfuse.com/v1/datapoint"),
	DefaultSource:     workarounds.GolangDoesnotAllowPointerToStringLiteral(""),
	MetricCreationURL: workarounds.GolangDoesnotAllowPointerToStringLiteral("https://api.signalfuse.com/v1/metric?bulkupdate=true"),
	TimeoutDuration:   workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	BufferSize:        workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(10000)),
	DrainingThreads:   workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(5)),
	Name:              workarounds.GolangDoesnotAllowPointerToStringLiteral("signalfx-forwarder"),
	MaxDrainSize:      workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(100)),
}

// SignalfxJSONForwarderLoader loads a json forwarder forwarding points from proxy to SignalFx
func SignalfxJSONForwarderLoader(forwardTo *config.ForwardTo) (core.StatKeepingStreamingAPI, error) {
	structdefaults.FillDefaultFrom(forwardTo, defaultConfig)
	glog.Infof("Creating signalfx forwarder using final config %s", forwardTo)
	return NewSignalfxJSONForwarer(*forwardTo.URL, *forwardTo.TimeoutDuration, *forwardTo.BufferSize,
		*forwardTo.DefaultAuthToken, *forwardTo.DrainingThreads, *forwardTo.Name, *forwardTo.MetricCreationURL,
		*forwardTo.MaxDrainSize, *forwardTo.DefaultSource)
}

// NewSignalfxJSONForwarer creates a new JSON forwarder
func NewSignalfxJSONForwarer(url string, timeout time.Duration, bufferSize uint32,
	defaultAuthToken string, drainingThreads uint32, name string, MetricCreationURL string,
	maxDrainSize uint32, defaultSource string) (core.StatKeepingStreamingAPI, error) {
	tr := &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		MaxIdleConnsPerHost: int(drainingThreads) * 2,
	}
	ret := &signalfxJSONConnector{
		basicBufferedForwarder: newBasicBufferedForwarder(bufferSize, maxDrainSize, name, drainingThreads),
		url:              url,
		defaultAuthToken: defaultAuthToken,
		sendVersion:      1,
		userAgent:        fmt.Sprintf("SignalfxProxy/0.1 (gover %s)", runtime.Version()),
		tr:               tr,
		client: &http.Client{
			Transport: tr,
		},
		connectionTimeout:   timeout,
		v1MetricLoadedCache: map[string]struct{}{},
		MetricCreationURL:   MetricCreationURL,
		defaultSource:       defaultSource,
		metricCreationChan:  make(chan *metricCreationRequest),
	}
	ret.start(ret.process)
	go ret.metricCreationLoop()
	return ret, nil
}

func (connector *signalfxJSONConnector) encodePostBodyV2(datapoints []core.Datapoint) ([]byte, string, error) {
	bodyToSend := make(map[string][]*BodySendFormat)
	for _, dp := range datapoints {
		bsf := &BodySendFormat{
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
			bodyToSend[dp.MetricType().String()] = make([]*BodySendFormat, 0)
		}
		bodyToSend[dp.MetricType().String()] = append(bodyToSend[dp.MetricType().String()], bsf)
	}
	jsonBytes, err := json.Marshal(&bodyToSend)
	glog.V(3).Infof("Posting %s from %s", jsonBytes, bodyToSend)

	// Now we can send datapoints
	return jsonBytes, "application/json", err
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
	jsonBytes, err := json.Marshal(&postBody)
	if err != nil {
		glog.Warningf("Unable to marshal body: %s", err)
		return err
	}
	glog.V(3).Infof("Posting %s from %s", jsonBytes, postBody)

	req, _ := http.NewRequest("POST", connector.MetricCreationURL, bytes.NewBuffer(jsonBytes))
	glog.V(3).Infof("Request is %s", req)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-SF-TOKEN", connector.defaultAuthToken)
	req.Header.Set("User-Agent", connector.userAgent)

	req.Header.Set("Connection", "Keep-Alive")
	resp, err := connector.client.Do(req)

	if err != nil {
		glog.Warningf("Unable to POST response: %s", err)
		return err
	}

	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Warningf("Unable to verify response body: %s", err)
		return err
	}
	if resp.StatusCode != 200 {
		glog.Warningf("Metric creation failed: %s", respBody)
		return fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}
	var metricCreationBody []protocoltypes.SignalfxMetricCreationResponse
	err = json.Unmarshal(respBody, &metricCreationBody)
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
	thisPointSource := point.Dimensions()["sf_source"]
	if thisPointSource != "" {
		return thisPointSource
	}
	return connector.defaultSource
}

func (connector *signalfxJSONConnector) encodePostBodyV1(datapoints []core.Datapoint) ([]byte, string, error) {
	var msgBody []byte
	metricsToBeCreated := make(map[string]com_signalfuse_metrics_protobuf.MetricType)
	connector.v1MetricLoadedCacheMutex.Lock()
	defer connector.v1MetricLoadedCacheMutex.Unlock()
	for _, point := range datapoints {
		thisPointSource := connector.figureOutReasonableSource(point)
		if thisPointSource == "" {
			glog.Warningf("unable to figure out a reasonable source for %s, skipping", point)
			continue
		}
		if point.MetricType() != com_signalfuse_metrics_protobuf.MetricType_GAUGE {
			_, preCreated := connector.v1MetricLoadedCache[point.Metric()]
			if !preCreated {
				metricsToBeCreated[point.Metric()] = point.MetricType()
			}
		}
		m := point.Metric()
		ts := point.Timestamp().UnixNano() / time.Millisecond.Nanoseconds()
		v := &com_signalfuse_metrics_protobuf.DataPoint{
			Source:    &thisPointSource,
			Metric:    &m,
			Timestamp: &ts,
			Value:     datumForPoint(point.Value()),
		}
		glog.V(3).Infof("Single datapoint to signalfx: %s", v)
		encodedBytes, err := proto.Marshal(v)
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
	glog.V(3).Infof("Request: %s from %s", req, string(jsonBytes))
	req.Header.Set("Content-Type", bodyType)
	req.Header.Set("X-SF-TOKEN", connector.defaultAuthToken)
	req.Header.Set("User-Agent", connector.userAgent)

	req.Header.Set("Connection", "Keep-Alive")
	resp, err := connector.client.Do(req)

	if err != nil {
		glog.Warningf("Unable to POST response: %s", err)
		return err
	}

	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Warningf("Unable to verify response body: %s", err)
		return err
	}
	if resp.StatusCode != 200 {
		glog.Warningf("Metric upload failed: %s", respBody)
		return fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}
	var body string
	err = json.Unmarshal(respBody, &body)
	if err != nil {
		glog.Warningf("body=(%s), err=(%s)", respBody, err)
		return err
	}
	if body != "OK" {
		glog.Warningf("Response not OK: %s", body)
		return err
	}
	return nil
}
