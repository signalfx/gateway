package signalfx

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"strings"
	"time"
	"unicode"

	"code.google.com/p/goprotobuf/proto"
	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/datapoint"
	"github.com/signalfuse/signalfxproxy/stats"
)

type signalfxJSONConnector struct {
	datapoint.BufferedForwarder
	url               string
	connectionTimeout time.Duration
	defaultAuthToken  string
	tr                *http.Transport
	client            *http.Client
	userAgent         string
	defaultSource     string
	dimensionSources  []string

	protoMarshal protoMarshalStub
}

type protoMarshalStub func(pb proto.Message) ([]byte, error)

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

// ForwarderLoader loads a json forwarder forwarding points from proxy to SignalFx
func ForwarderLoader(forwardTo *config.ForwardTo) (stats.StatKeepingStreamer, error) {
	if forwardTo.FormatVersion == nil {
		forwardTo.FormatVersion = workarounds.GolangDoesnotAllowPointerToUintLiteral(3)
	}
	if *forwardTo.FormatVersion == 1 {
		log.WithField("forwardTo", forwardTo).Warn("Old formats not supported in signalfxforwarder.  Using newer format.  Please update config to use format version 2 or 3")
	}
	structdefaults.FillDefaultFrom(forwardTo, defaultConfigV2)
	log.WithField("forwardTo", forwardTo).Info("Creating signalfx forwarder using final config")
	return NewSignalfxJSONForwarer(*forwardTo.URL, *forwardTo.TimeoutDuration, *forwardTo.BufferSize,
		*forwardTo.DefaultAuthToken, *forwardTo.DrainingThreads, *forwardTo.Name,
		*forwardTo.MaxDrainSize, *forwardTo.DefaultSource, *forwardTo.SourceDimensions)
}

// NewSignalfxJSONForwarer creates a new JSON forwarder
func NewSignalfxJSONForwarer(url string, timeout time.Duration, bufferSize uint32,
	defaultAuthToken string, drainingThreads uint32, name string,
	maxDrainSize uint32, defaultSource string, sourceDimensions string) (stats.StatKeepingStreamer, error) {
	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		MaxIdleConnsPerHost:   int(drainingThreads) * 2,
		ResponseHeaderTimeout: timeout,
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, timeout)
		},
	}
	ret := &signalfxJSONConnector{
		BufferedForwarder: *datapoint.NewBufferedForwarder(bufferSize, maxDrainSize, name, drainingThreads),
		url:               url,
		defaultAuthToken:  defaultAuthToken,
		userAgent:         fmt.Sprintf("SignalfxProxy/0.3 (gover %s)", runtime.Version()),
		tr:                tr,
		client: &http.Client{
			Transport: tr,
		},
		connectionTimeout: timeout,
		defaultSource:     defaultSource,
		// sf_source is always a dimension that can be a source
		dimensionSources: append([]string{"sf_source"}, strings.Split(sourceDimensions, ",")...),
	}
	ret.Start(ret.process)
	return ret, nil
}

func (connector *signalfxJSONConnector) encodePostBodyProtobufV2(datapoints []datapoint.Datapoint) ([]byte, string, error) {
	dps := make([]*com_signalfuse_metrics_protobuf.DataPoint, 0, len(datapoints))
	for _, dp := range datapoints {
		dps = append(dps, connector.coreDatapointToProtobuf(dp))
	}
	msg := &com_signalfuse_metrics_protobuf.DataPointUploadMessage{
		Datapoints: dps,
	}
	protoMarshal := connector.protoMarshal
	if protoMarshal == nil {
		protoMarshal = proto.Marshal
	}
	protobytes, err := protoMarshal(msg)

	// Now we can send datapoints
	return protobytes, "application/x-protobuf", err
}

func datumForPoint(pv datapoint.Value) *com_signalfuse_metrics_protobuf.Datum {

	switch t := pv.(type) {
	case datapoint.IntValue:
		x := t.Int()
		return &com_signalfuse_metrics_protobuf.Datum{IntValue: &x}
	case datapoint.FloatValue:
		x := t.Float()
		return &com_signalfuse_metrics_protobuf.Datum{DoubleValue: &x}
	default:
		x := t.String()
		return &com_signalfuse_metrics_protobuf.Datum{StrValue: &x}
	}
}

func (connector *signalfxJSONConnector) figureOutReasonableSource(point datapoint.Datapoint) string {
	for _, sourceName := range connector.dimensionSources {
		thisPointSource := point.Dimensions()[sourceName]
		if thisPointSource != "" {
			return thisPointSource
		}
	}
	return connector.defaultSource
}

func (connector *signalfxJSONConnector) coreDatapointToProtobuf(point datapoint.Datapoint) *com_signalfuse_metrics_protobuf.DataPoint {
	thisPointSource := connector.figureOutReasonableSource(point)
	m := point.Metric()
	ts := point.Timestamp().UnixNano() / time.Millisecond.Nanoseconds()
	relativeTimeDp, ok := point.(datapoint.TimeRelativeDatapoint)
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
	ret := make([]*com_signalfuse_metrics_protobuf.Dimension, 0, len(dimensions))
	for k, v := range dimensions {
		if k == "" || v == "" {
			continue
		}
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

func runeFilterMap(r rune) rune {
	if unicode.IsDigit(r) || unicode.IsLetter(r) || r == '_' {
		return r
	}
	return '_'
}

func filterSignalfxKey(str string) string {
	return strings.Map(runeFilterMap, str)
}

func (connector *signalfxJSONConnector) Stats() []datapoint.Datapoint {
	return connector.BufferedForwarder.Stats()
}

func (connector *signalfxJSONConnector) process(datapoints []datapoint.Datapoint) error {
	log.WithField("len", len(datapoints)).Debug("Processing dp")
	jsonBytes, bodyType, err := connector.encodePostBodyProtobufV2(datapoints)

	if err != nil {
		log.WithField("err", err).Warn("Unable to marshal object")
		return err
	}
	req, _ := http.NewRequest("POST", connector.url, bytes.NewBuffer(jsonBytes))
	req.Header.Set("Content-Type", bodyType)
	req.Header.Set("X-SF-TOKEN", connector.defaultAuthToken)
	req.Header.Set("User-Agent", connector.userAgent)

	req.Header.Set("Connection", "Keep-Alive")
	resp, err := connector.client.Do(req)

	if err != nil {
		log.WithField("err", err).Warn("Unable to POST request")
		return err
	}

	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WithField("err", err).Warn("Unable to verify response body")
		return err
	}
	if resp.StatusCode != 200 {
		log.WithFields(log.Fields{"url": connector.url, "respBody": string(respBody)}).Warn("Metric upload failed")
		return fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}
	var body string
	err = json.Unmarshal(respBody, &body)
	if err != nil {
		log.WithFields(log.Fields{"body": string(respBody), "err": err}).Warn("Unable to unmarshal")
		return err
	}
	if body != "OK" {
		log.WithField("body", body).Warn("Response not OK")
		return fmt.Errorf("Body decode error: %s", body)
	}
	return nil
}
