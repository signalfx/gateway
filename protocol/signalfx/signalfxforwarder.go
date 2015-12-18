package signalfx

import (
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

	"sync/atomic"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/golang/protobuf/proto"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/dp/dpbuffered"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/stats"
	"golang.org/x/net/context"
)

// Forwarder controls forwarding datapoints to SignalFx
type Forwarder struct {
	propertyLock          sync.Mutex
	url                   string
	eventURL              string
	defaultAuthToken      string
	tr                    *http.Transport
	client                *http.Client
	userAgent             string
	defaultSource         string
	dimensionSources      []string
	emptyMetricNameFilter dpsink.EmptyMetricFilter

	datapointSink dpsink.DSink

	protoMarshal func(pb proto.Message) ([]byte, error)
	jsonMarshal  func(v interface{}) ([]byte, error)
}

var defaultConfigV2 = &config.ForwardTo{
	URL:               workarounds.GolangDoesnotAllowPointerToStringLiteral("https://ingest.signalfx.com/v2/datapoint"),
	EventURL:          workarounds.GolangDoesnotAllowPointerToStringLiteral("https://ingest.signalfx.com/v2/event"),
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
	proxyVersion, ok := ctx.Value("version").(string)
	if !ok || proxyVersion == "" {
		proxyVersion = "UNKNOWN_VERSION"
	}
	if forwardTo.FormatVersion == nil {
		forwardTo.FormatVersion = workarounds.GolangDoesnotAllowPointerToUintLiteral(3)
	}
	if *forwardTo.FormatVersion != 3 {
		return nil, nil, errors.New("old formats not supported in signalfxforwarder: update config to use format 3")
	}
	structdefaults.FillDefaultFrom(forwardTo, defaultConfigV2)
	log.WithField("forwardTo", forwardTo).Info("Creating signalfx forwarder using final config")
	fwd := NewSignalfxJSONForwarder(*forwardTo.URL, *forwardTo.TimeoutDuration,
		*forwardTo.DefaultAuthToken, *forwardTo.DrainingThreads,
		*forwardTo.DefaultSource, *forwardTo.SourceDimensions, proxyVersion)
	fwd.eventURL = *forwardTo.EventURL
	counter := &dpsink.Counter{}
	dims := protocol.ForwarderDims(*forwardTo.Name, "sfx_protobuf_v2")
	buffer := dpbuffered.NewBufferedForwarder(ctx, *(&dpbuffered.Config{}).FromConfig(forwardTo), fwd)
	return &protocol.CompositeForwarder{
		Sink:   dpsink.FromChain(buffer, dpsink.NextWrap(counter)),
		Keeper: stats.ToKeeperMany(dims, counter, buffer),
		Closer: protocol.CompositeCloser(protocol.OkCloser(buffer.Close)),
	}, fwd, nil
}

// NewSignalfxJSONForwarder creates a new JSON forwarder
func NewSignalfxJSONForwarder(url string, timeout time.Duration,
	defaultAuthToken string, drainingThreads uint32,
	defaultSource string, sourceDimensions string, proxyVersion string) *Forwarder {
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConnsPerHost:   int(drainingThreads) * 2,
		ResponseHeaderTimeout: timeout,
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, timeout)
		},
		TLSHandshakeTimeout: timeout,
	}
	datapointSendingSink := sfxclient.NewHTTPDatapointSink()
	datapointSendingSink.Client.Timeout = timeout
	datapointSendingSink.Client = http.Client{
		Transport: tr,
	}
	datapointSendingSink.AuthToken = defaultAuthToken
	datapointSendingSink.UserAgent = fmt.Sprintf("SignalfxProxy/%s (gover %s)", proxyVersion, runtime.Version())
	datapointSendingSink.Endpoint = url
	ret := &Forwarder{
		url:              url,
		defaultAuthToken: datapointSendingSink.AuthToken,
		userAgent:        datapointSendingSink.UserAgent,
		tr:               tr,
		client:           &datapointSendingSink.Client,
		protoMarshal:     proto.Marshal,
		jsonMarshal:      json.Marshal,
		datapointSink:    datapointSendingSink,
		defaultSource:    defaultSource,
		// sf_source is always a dimension that can be a source
		dimensionSources: append([]string{"sf_source"}, strings.Split(sourceDimensions, ",")...),
	}
	return ret
}

func (connector *Forwarder) encodeEventPostBodyProtobufV2(events []*event.Event) ([]byte, string, error) {
	evts := make([]*com_signalfx_metrics_protobuf.Event, 0, len(events))
	for _, evt := range events {
		evts = append(evts, connector.coreEventToProtobuf(evt))
	}
	msg := &com_signalfx_metrics_protobuf.EventUploadMessage{
		Events: evts,
	}
	protobytes, err := connector.protoMarshal(msg)

	// Now we can send events
	return protobytes, "application/x-protobuf", err
}

func (connector *Forwarder) coreEventToProtobuf(e *event.Event) *com_signalfx_metrics_protobuf.Event {
	ts := e.Timestamp.UnixNano() / time.Millisecond.Nanoseconds()
	et := e.EventType
	cat := com_signalfx_metrics_protobuf.EventCategory_USER_DEFINED
	if catIndex, ok := com_signalfx_metrics_protobuf.EventCategory_value[e.Category]; ok {
		cat = com_signalfx_metrics_protobuf.EventCategory(catIndex)
	}

	v := &com_signalfx_metrics_protobuf.Event{
		EventType:  &et,
		Timestamp:  &ts,
		Properties: mapToProperties(e.Meta),
		Dimensions: mapToDimensions(e.Dimensions),
		Category:   &cat,
	}
	return v
}

func mapToProperties(properties map[string]interface{}) []*com_signalfx_metrics_protobuf.Property {
	ret := make([]*com_signalfx_metrics_protobuf.Property, 0, len(properties))
	for k, v := range properties {
		if k == "" || v == nil {
			continue
		}
		copyOfK := filterSignalfxKey(k)

		pv := com_signalfx_metrics_protobuf.PropertyValue{}
		if ival, ok := v.(int64); ok {
			pv.IntValue = &ival
		} else if bval, ok := v.(bool); ok {
			pv.BoolValue = &bval
		} else if dval, ok := v.(float64); ok {
			pv.DoubleValue = &dval
		} else if sval, ok := v.(string); ok {
			pv.StrValue = &sval
		} else {
			// ignore, shouldn't be possible to get here from external source
			continue
		}

		ret = append(ret, (&com_signalfx_metrics_protobuf.Property{
			Key:   &copyOfK,
			Value: &pv,
		}))
	}
	return ret
}
func mapToDimensions(dimensions map[string]string) []*com_signalfx_metrics_protobuf.Dimension {
	ret := make([]*com_signalfx_metrics_protobuf.Dimension, 0, len(dimensions))
	for k, v := range dimensions {
		if k == "" || v == "" {
			continue
		}
		copyOfK := filterSignalfxKey(k)
		copyOfV := v

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

// EventEndpoint sets where events are sent
func (connector *Forwarder) EventEndpoint(endpoint string) {
	connector.propertyLock.Lock()
	defer connector.propertyLock.Unlock()
	connector.eventURL = endpoint
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

	datapoints = connector.emptyMetricNameFilter.FilterDatapoints(datapoints)
	if len(datapoints) == 0 {
		return nil
	}
	return connector.datapointSink.AddDatapoints(ctx, datapoints)
}

// AddEvents forwards events to SignalFx
func (connector *Forwarder) AddEvents(ctx context.Context, events []*event.Event) error {
	connector.propertyLock.Lock()
	endpoint := connector.eventURL
	userAgent := connector.userAgent
	defaultAuthToken := connector.defaultAuthToken
	connector.propertyLock.Unlock()

	// could filter here
	if len(events) == 0 {
		return nil
	}
	protoBytes, bodyType, err := connector.encodeEventPostBodyProtobufV2(events)
	if err != nil {
		return &forwardError{
			originalError: err,
			message:       "Unable to marshal object",
		}
	}
	return connector.sendBytes(endpoint, bodyType, defaultAuthToken, userAgent, protoBytes)
}

var atomicRequestNumber = int64(0)

// TODO(mwp): Move event adds to sfxclient
func (connector *Forwarder) sendBytes(endpoint string, bodyType string, defaultAuthToken string, userAgent string, jsonBytes []byte) error {
	req, _ := http.NewRequest("POST", endpoint, bytes.NewBuffer(jsonBytes))
	req.Header.Set("Content-Type", bodyType)
	req.Header.Set(TokenHeaderName, defaultAuthToken)
	req.Header.Set("User-Agent", userAgent)

	req.Header.Set("Connection", "Keep-Alive")

	if log.GetLevel() <= log.DebugLevel {
		reqN := atomic.AddInt64(&atomicRequestNumber, 1)
		log.WithField("req#", reqN).WithField("header", req.Header).WithField("body-len", len(jsonBytes)).Debug("Sending a request")
		defer func() {
			log.WithField("req#", reqN).Debug("Done sending request")
		}()
	}

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
