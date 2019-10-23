package opencensus

import (
	"context"
	"github.com/open-telemetry/opentelemetry-collector/config/configgrpc"
	"github.com/open-telemetry/opentelemetry-collector/exporter/opencensusexporter"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/trace"
	"io"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"
)

// ForwarderConfig controls optional parameters for an opencensus forwarder
type ForwarderConfig struct {
	TraceEndpoint     *string
	AdditionalHeaders map[string]string
	ReconnectionDelay *time.Duration
	Compression       *string
	NumWorkers        *int
	Logger            log.Logger
	UseSecure         *bool
	Retries           *int
	ClusterName       *string
}

var defaultForwarderConfig = &ForwarderConfig{
	TraceEndpoint:     pointer.String(""),
	AdditionalHeaders: map[string]string{},
	ReconnectionDelay: pointer.Duration(time.Second * 2),
	Compression:       pointer.String("gzip"),
	NumWorkers:        pointer.Int(runtime.NumCPU()),
	UseSecure:         pointer.Bool(true),
	Logger:            log.Discard,
	Retries:           pointer.Int(3),
	ClusterName:       pointer.String(""),
}

type cSink interface {
	trace.Sink
	io.Closer
}

// Forwarder controls forwarding traces to to an opencensus exporter
type Forwarder struct {
	sink    cSink
	Logger  log.Logger
	stats   stats
	cluster string
}

func (connector *Forwarder) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	return nil
}

func (connector *Forwarder) AddEvents(ctx context.Context, events []*event.Event) error {
	return nil
}

func addTags(tags map[string]string, additionalTags ...string) map[string]string {
	if tags == nil {
		tags = make(map[string]string, len(additionalTags)/2)
	}
	for i := 0; i < len(additionalTags)-1; i += 2 {
		tags[additionalTags[i]] = additionalTags[i+1]
	}
	return tags
}

func (connector *Forwarder) AddSpans(ctx context.Context, spans []*trace.Span) error {
	atomic.AddInt64(&connector.stats.totalSpansForwarded, int64(len(spans)))
	connector.stats.drainSize.Add(float64(len(spans)))
	for _, s := range spans {
		s.Tags = addTags(s.Tags, "sf_cluster", connector.cluster)
	}
	return connector.sink.AddSpans(ctx, spans)
}

func (connector *Forwarder) Pipeline() int64 {
	return 0 // not sure how to do this
}

func (connector *Forwarder) Close() error {
	return connector.sink.Close()
}

func (connector *Forwarder) StartupFinished() error {
	return nil
}

// DebugDatapoints returns datapoints that are used for debugging
func (connector *Forwarder) DebugDatapoints() []*datapoint.Datapoint {
	dps := connector.stats.drainSize.Datapoints()
	dps = append(dps, sfxclient.Cumulative("totalSpansForwarded", map[string]string{"direction": "forwarder", "destination": "opencensus"}, atomic.LoadInt64(&connector.stats.totalSpansForwarded)))
	return dps
}

// DefaultDatapoints returns a set of default datapoints about the forwarder
func (connector *Forwarder) DefaultDatapoints() []*datapoint.Datapoint {
	return nil
}

// Datapoints implements the sfxclient.Collector interface and returns all datapoints
func (connector *Forwarder) Datapoints() []*datapoint.Datapoint {
	return append(connector.DebugDatapoints(), connector.DefaultDatapoints()...)
}

// DebugEndpoints returns empty debug endpoints currently
func (connector *Forwarder) DebugEndpoints() map[string]http.Handler {
	return nil
}

type stats struct {
	drainSize           *sfxclient.RollingBucket
	totalSpansForwarded int64
}

// NewForwarder creates a new opencensus forwarder
func NewForwarder(conf *ForwarderConfig) (ret *Forwarder, err error) {
	conf = pointer.FillDefaultFrom(conf, defaultForwarderConfig).(*ForwarderConfig)

	config := &opencensusexporter.Config{
		GRPCSettings: configgrpc.GRPCSettings{
			Endpoint:    *conf.TraceEndpoint,
			Headers:     conf.AdditionalHeaders,
			Compression: *conf.Compression,
			UseSecure:   *conf.UseSecure,
		},
		NumWorkers:        *conf.NumWorkers,
		ReconnectionDelay: *conf.ReconnectionDelay,
	}
	sendingSink, err := sfxclient.NewOCSink(config, *conf.Retries)
	if err == nil {
		ret = &Forwarder{
			sink:    sendingSink,
			Logger:  conf.Logger,
			cluster: *conf.ClusterName,
			stats: stats{
				drainSize: sfxclient.NewRollingBucket("drain_size", map[string]string{
					"direction":   "forwarder",
					"destination": "opencensus",
				}),
			},
		}
	}
	return ret, err
}
