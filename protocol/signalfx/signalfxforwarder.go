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

	"github.com/golang/protobuf/proto"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/metricproxy/protocol/common"
	"golang.org/x/net/context"
	"sync/atomic"
)

// Forwarder controls forwarding datapoints to SignalFx
type Forwarder struct {
	filtering.FilteredForwarder
	propertyLock          sync.Mutex
	eventURL              string
	defaultAuthToken      string
	tr                    *http.Transport
	client                *http.Client
	userAgent             string
	emptyMetricNameFilter dpsink.EmptyMetricFilter

	datapointSink dpsink.DSink

	protoMarshal func(pb proto.Message) ([]byte, error)
	jsonMarshal  func(v interface{}) ([]byte, error)
	Logger       log.Logger
	stats        stats
}

type stats struct {
	totalDatapointsForwarded int64
	totalEventsForwarded     int64
}

// ForwarderConfig controls optional parameters for a signalfx forwarder
type ForwarderConfig struct {
	Filters          *filtering.FilterObj
	DatapointURL     *string
	EventURL         *string
	Timeout          *time.Duration
	SourceDimensions *string
	ProxyVersion     *string
	MaxIdleConns     *int64
	AuthToken        *string
	ProtoMarshal     func(pb proto.Message) ([]byte, error)
	JSONMarshal      func(v interface{}) ([]byte, error)
	Logger           log.Logger
}

var defaultForwarderConfig = &ForwarderConfig{
	Filters:      &filtering.FilterObj{},
	DatapointURL: pointer.String("https://ingest.signalfx.com/v2/datapoint"),
	EventURL:     pointer.String("https://ingest.signalfx.com/v2/event"),
	AuthToken:    pointer.String(""),
	Timeout:      pointer.Duration(time.Second * 30),
	ProxyVersion: pointer.String("UNKNOWN_VERSION"),
	MaxIdleConns: pointer.Int64(20),
	ProtoMarshal: proto.Marshal,
	JSONMarshal:  json.Marshal,
	Logger:       log.Discard,
}

// NewForwarder creates a new JSON forwarder
func NewForwarder(conf *ForwarderConfig) *Forwarder {
	conf = pointer.FillDefaultFrom(conf, defaultForwarderConfig).(*ForwarderConfig)
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConnsPerHost:   int(*conf.MaxIdleConns * 2),
		ResponseHeaderTimeout: *conf.Timeout,
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, *conf.Timeout)
		},
		TLSHandshakeTimeout: *conf.Timeout,
	}
	datapointSendingSink := sfxclient.NewHTTPDatapointSink()
	datapointSendingSink.Client = http.Client{
		Transport: tr,
		Timeout:   *conf.Timeout,
	}
	datapointSendingSink.AuthToken = *conf.AuthToken
	datapointSendingSink.UserAgent = fmt.Sprintf("SignalfxProxy/%s (gover %s)", *conf.ProxyVersion, runtime.Version())
	datapointSendingSink.Endpoint = *conf.DatapointURL
	ret := &Forwarder{
		defaultAuthToken: datapointSendingSink.AuthToken,
		userAgent:        datapointSendingSink.UserAgent,
		tr:               tr,
		client:           &datapointSendingSink.Client,
		protoMarshal:     conf.ProtoMarshal,
		eventURL:         *conf.EventURL,
		jsonMarshal:      conf.JSONMarshal,
		datapointSink:    datapointSendingSink,
		Logger:           conf.Logger,
	}
	ret.Setup(conf.Filters)
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

// Datapoints returns nothing.
func (connector *Forwarder) Datapoints() []*datapoint.Datapoint {
	return connector.GetFilteredDatapoints()
}

// Close will terminate idle HTTP client connections
func (connector *Forwarder) Close() error {
	connector.tr.CloseIdleConnections()
	return nil
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
		Properties: mapToProperties(e.Properties),
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

// TokenHeaderName is the header key for the auth token in the HTTP request
const TokenHeaderName = "X-SF-TOKEN"

// AddDatapoints forwards datapoints to SignalFx
func (connector *Forwarder) AddDatapoints(ctx context.Context, datapoints []*datapoint.Datapoint) error {
	atomic.AddInt64(&connector.stats.totalDatapointsForwarded, int64(len(datapoints)))
	datapoints = connector.emptyMetricNameFilter.FilterDatapoints(datapoints)
	datapoints = connector.FilterDatapoints(datapoints)
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

	atomic.AddInt64(&connector.stats.totalEventsForwarded, int64(len(events)))
	// could filter here
	if len(events) == 0 {
		return nil
	}
	protoBytes, bodyType, err := connector.encodeEventPostBodyProtobufV2(events)
	if err != nil {
		return errors.Annotate(err, "unable to marshal object")
	}
	return connector.sendBytes(endpoint, bodyType, defaultAuthToken, userAgent, protoBytes)
}

func checkResp(resp *http.Response) error {
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Annotate(err, "unable to verify response body")
	}
	if resp.StatusCode != 200 {
		return errors.Errorf("invalid status code: %d", resp.StatusCode)
	}
	var body string
	err = json.Unmarshal(respBody, &body)
	if err != nil {
		return errors.Annotate(err, string(respBody))
	}
	if body != "OK" {
		return errors.Errorf("Resp body not ok: %s", respBody)
	}
	return nil
}

// Pipeline returns the total of all things forwarded
func (connector *Forwarder) Pipeline() int64 {
	return atomic.LoadInt64(&connector.stats.totalDatapointsForwarded) + atomic.LoadInt64(&connector.stats.totalEventsForwarded)
}

// TODO(mwp): Move event adds to sfxclient
func (connector *Forwarder) sendBytes(endpoint string, bodyType string, defaultAuthToken string, userAgent string, jsonBytes []byte) error {
	req, _ := http.NewRequest("POST", endpoint, bytes.NewReader(jsonBytes))
	req.Header.Set("Content-Type", bodyType)
	req.Header.Set(TokenHeaderName, defaultAuthToken)
	req.Header.Set("User-Agent", userAgent)

	req.Header.Set("Connection", "Keep-Alive")

	// TODO: Set timeout from ctx
	resp, err := connector.client.Do(req)

	if err != nil {
		return errors.Annotate(err, "unable to POST request")
	}

	defer func() {
		log.IfErr(connector.Logger, resp.Body.Close())
	}()
	return checkResp(resp)
}
