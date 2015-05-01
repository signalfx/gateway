package signalfx

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"time"
	"unicode"

	"bytes"
	"encoding/json"
	"io/ioutil"

	"sync"

	"errors"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/golang/protobuf/proto"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/dp/dpbuffered"
	"github.com/signalfx/metricproxy/dp/dpsink"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/stats"
	"golang.org/x/net/context"
)

// Forwarder controls forwarding datapoints to SignalFx
type Forwarder struct {
	propertyLock          sync.Mutex
	url                   string
	defaultAuthToken      string
	tr                    *http.Transport
	client                *http.Client
	userAgent             string
	defaultSource         string
	dimensionSources      []string
	emptyMetricNameFilter dpsink.EmptyMetricFilter

	protoMarshal func(pb proto.Message) ([]byte, error)
}

var defaultConfigV2 = &config.ForwardTo{
	URL:               workarounds.GolangDoesnotAllowPointerToStringLiteral("https://ingest.signalfx.com/v2/datapoint"),
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
func ForwarderLoader(ctx context.Context, forwardTo *config.ForwardTo) (protocol.Forwarder, error) {
	f, _, err := ForwarderLoader1(ctx, forwardTo)
	return f, err
}

// ForwarderLoader1 is a more strictly typed version of ForwarderLoader
func ForwarderLoader1(ctx context.Context, forwardTo *config.ForwardTo) (protocol.Forwarder, *Forwarder, error) {
	if forwardTo.FormatVersion == nil {
		forwardTo.FormatVersion = workarounds.GolangDoesnotAllowPointerToUintLiteral(3)
	}
	if *forwardTo.FormatVersion == 1 {
		log.WithField("forwardTo", forwardTo).Warn("Old formats not supported in signalfxforwarder.  Using newer format.  Please update config to use format version 2 or 3")
	}
	structdefaults.FillDefaultFrom(forwardTo, defaultConfigV2)
	log.WithField("forwardTo", forwardTo).Info("Creating signalfx forwarder using final config")
	fwd := NewSignalfxJSONForwarer(*forwardTo.URL, *forwardTo.TimeoutDuration,
		*forwardTo.DefaultAuthToken, *forwardTo.DrainingThreads,
		*forwardTo.DefaultSource, *forwardTo.SourceDimensions)

	counter := &dpsink.Counter{}
	dims := map[string]string{
		"name":     *forwardTo.Name,
		"location": "forwarder",
		"type":     "signalfx",
	}
	buffer := dpbuffered.NewBufferedForwarder(ctx, *(&dpbuffered.Config{}).FromConfig(forwardTo), fwd)
	return &protocol.CompositeForwarder{
		Sink:   dpsink.FromChain(buffer, dpsink.NextWrap(counter)),
		Keeper: stats.ToKeeperMany(dims, counter, buffer),
		Closer: protocol.CompositeCloser(protocol.OkCloser(buffer.Close)),
	}, fwd, nil
}

// NewSignalfxJSONForwarer creates a new JSON forwarder
func NewSignalfxJSONForwarer(url string, timeout time.Duration,
	defaultAuthToken string, drainingThreads uint32,
	defaultSource string, sourceDimensions string) *Forwarder {
	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		MaxIdleConnsPerHost:   int(drainingThreads) * 2,
		ResponseHeaderTimeout: timeout,
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, timeout)
		},
	}
	ret := &Forwarder{
		url:              url,
		defaultAuthToken: defaultAuthToken,
		userAgent:        fmt.Sprintf("SignalfxProxy/0.3 (gover %s)", runtime.Version()),
		tr:               tr,
		client: &http.Client{
			Transport: tr,
		},
		protoMarshal:  proto.Marshal,
		defaultSource: defaultSource,
		// sf_source is always a dimension that can be a source
		dimensionSources: append([]string{"sf_source"}, strings.Split(sourceDimensions, ",")...),
	}
	return ret
}

func (connector *Forwarder) encodePostBodyProtobufV2(datapoints []*datapoint.Datapoint) ([]byte, string, error) {
	dps := make([]*com_signalfx_metrics_protobuf.DataPoint, 0, len(datapoints))
	for _, dp := range datapoints {
		dps = append(dps, connector.coreDatapointToProtobuf(dp))
	}
	msg := &com_signalfx_metrics_protobuf.DataPointUploadMessage{
		Datapoints: dps,
	}
	protobytes, err := connector.protoMarshal(msg)

	// Now we can send datapoints
	return protobytes, "application/x-protobuf", err
}

func datumForPoint(pv datapoint.Value) *com_signalfx_metrics_protobuf.Datum {
	switch t := pv.(type) {
	case datapoint.IntValue:
		x := t.Int()
		return &com_signalfx_metrics_protobuf.Datum{IntValue: &x}
	case datapoint.FloatValue:
		x := t.Float()
		return &com_signalfx_metrics_protobuf.Datum{DoubleValue: &x}
	default:
		x := t.String()
		return &com_signalfx_metrics_protobuf.Datum{StrValue: &x}
	}
}

func (connector *Forwarder) figureOutReasonableSource(point *datapoint.Datapoint) string {
	for _, sourceName := range connector.dimensionSources {
		thisPointSource := point.Dimensions[sourceName]
		if thisPointSource != "" {
			return thisPointSource
		}
	}
	return connector.defaultSource
}

func (connector *Forwarder) coreDatapointToProtobuf(point *datapoint.Datapoint) *com_signalfx_metrics_protobuf.DataPoint {
	thisPointSource := connector.figureOutReasonableSource(point)
	m := point.Metric
	ts := point.Timestamp.UnixNano() / time.Millisecond.Nanoseconds()
	mt := toMT(point.MetricType)
	v := &com_signalfx_metrics_protobuf.DataPoint{
		Metric:     &m,
		Timestamp:  &ts,
		Value:      datumForPoint(point.Value),
		MetricType: &mt,
		Dimensions: mapToDimensions(point.Dimensions),
	}
	if thisPointSource != "" {
		v.Source = &thisPointSource
	}
	return v
}

func mapToDimensions(dimensions map[string]string) []*com_signalfx_metrics_protobuf.Dimension {
	ret := make([]*com_signalfx_metrics_protobuf.Dimension, 0, len(dimensions))
	for k, v := range dimensions {
		if k == "" || v == "" {
			continue
		}
		// If someone knows a better way to do this, let me know.  I can't just take the &
		// of k and v because their content changes as the range iterates
		copyOfK := filterSignalfxKey(string([]byte(k)))
		copyOfV := (string([]byte(v)))
		ret = append(ret, (&com_signalfx_metrics_protobuf.Dimension{
			Key:   &copyOfK,
			Value: &copyOfV,
		}))
	}
	return ret
}

func filterSignalfxKey(str string) string {
	return strings.Map(runeFilterMap, str)
}

func runeFilterMap(r rune) rune {
	if unicode.IsDigit(r) || unicode.IsLetter(r) || r == '_' {
		return r
	}
	return '_'
}

// Endpoint sets where metrics are sent
func (connector *Forwarder) Endpoint(endpoint string) {
	connector.propertyLock.Lock()
	defer connector.propertyLock.Unlock()
	connector.url = endpoint
}

// UserAgent sets the User-Agent header on the request
func (connector *Forwarder) UserAgent(ua string) {
	connector.propertyLock.Lock()
	defer connector.propertyLock.Unlock()
	connector.userAgent = ua
}

// AuthToken identifies who is sending the request
func (connector *Forwarder) AuthToken(authToken string) {
	connector.propertyLock.Lock()
	defer connector.propertyLock.Unlock()
	connector.defaultAuthToken = authToken
}

// TokenHeaderName is the header key for the auth token in the HTTP request
const TokenHeaderName = "X-SF-TOKEN"

type forwardError struct {
	originalError error
	message       string
}

func (f *forwardError) Error() string {
	return fmt.Sprintf("%s: %s", f.message, f.originalError.Error())
}

var _ error = &forwardError{}

// AddDatapoints forwards datapoints to SignalFx
func (connector *Forwarder) AddDatapoints(ctx context.Context, datapoints []*datapoint.Datapoint) error {
	connector.propertyLock.Lock()
	endpoint := connector.url
	userAgent := connector.userAgent
	defautlAuthToken := connector.defaultAuthToken
	connector.propertyLock.Unlock()
	datapoints = connector.emptyMetricNameFilter.FilterDatapoints(datapoints)
	if len(datapoints) == 0 {
		return nil
	}
	jsonBytes, bodyType, err := connector.encodePostBodyProtobufV2(datapoints)

	if err != nil {
		return &forwardError{
			originalError: err,
			message:       "Unable to marshal object",
		}
	}
	req, _ := http.NewRequest("POST", endpoint, bytes.NewBuffer(jsonBytes))
	req.Header.Set("Content-Type", bodyType)
	req.Header.Set(TokenHeaderName, defautlAuthToken)
	req.Header.Set("User-Agent", userAgent)

	req.Header.Set("Connection", "Keep-Alive")

	// TODO: Set timeout from ctx
	resp, err := connector.client.Do(req)

	if err != nil {
		return &forwardError{
			originalError: err,
			message:       "Unable to POST request",
		}
	}

	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return &forwardError{
			originalError: err,
			message:       "Unable to verify response body",
		}
	}
	if resp.StatusCode != 200 {
		return &forwardError{
			originalError: fmt.Errorf("invalid status code: %d", resp.StatusCode),
			message:       string(respBody),
		}
	}
	var body string
	err = json.Unmarshal(respBody, &body)
	if err != nil {
		return &forwardError{
			originalError: err,
			message:       string(respBody),
		}
	}
	if body != "OK" {
		return &forwardError{
			originalError: errors.New("body decode error"),
			message:       body,
		}
	}
	return nil
}
