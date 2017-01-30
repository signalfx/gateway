package signalfx

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/metricproxy/protocol/filtering"
	"golang.org/x/net/context"
	"net"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"
)

// Forwarder controls forwarding datapoints to SignalFx
type Forwarder struct {
	filtering.FilteredForwarder
	defaultAuthToken      string
	tr                    *http.Transport
	client                *http.Client
	userAgent             string
	emptyMetricNameFilter dpsink.EmptyMetricFilter

	sink dpsink.Sink

	jsonMarshal func(v interface{}) ([]byte, error)
	Logger      log.Logger
	stats       stats
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
	JSONMarshal:  json.Marshal,
	Logger:       log.Discard,
}

// NewForwarder creates a new JSON forwarder
func NewForwarder(conf *ForwarderConfig) (*Forwarder, error) {
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
	datapointSendingSink := sfxclient.NewHTTPSink()
	datapointSendingSink.Client = http.Client{
		Transport: tr,
		Timeout:   *conf.Timeout,
	}
	datapointSendingSink.AuthToken = *conf.AuthToken
	datapointSendingSink.UserAgent = fmt.Sprintf("SignalfxProxy/%s (gover %s)", *conf.ProxyVersion, runtime.Version())
	datapointSendingSink.DatapointEndpoint = *conf.DatapointURL
	datapointSendingSink.EventEndpoint = *conf.EventURL
	ret := &Forwarder{
		defaultAuthToken: datapointSendingSink.AuthToken,
		userAgent:        datapointSendingSink.UserAgent,
		tr:               tr,
		client:           &datapointSendingSink.Client,
		jsonMarshal:      conf.JSONMarshal,
		sink:             datapointSendingSink,
		Logger:           conf.Logger,
	}
	err := ret.Setup(conf.Filters)
	if err != nil {
		return nil, err
	}
	return ret, nil
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
	return connector.sink.AddDatapoints(ctx, datapoints)
}

// AddEvents forwards events to SignalFx
func (connector *Forwarder) AddEvents(ctx context.Context, events []*event.Event) error {
	atomic.AddInt64(&connector.stats.totalEventsForwarded, int64(len(events)))
	// could filter here
	if len(events) == 0 {
		return nil
	}
	return connector.sink.AddEvents(ctx, events)
}

// Pipeline returns the total of all things forwarded
func (connector *Forwarder) Pipeline() int64 {
	return atomic.LoadInt64(&connector.stats.totalDatapointsForwarded) + atomic.LoadInt64(&connector.stats.totalEventsForwarded)
}
