package sampling

import (
	"context"

	"net/http"

	"github.com/signalfx/gateway/etcdIntf"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/trace"
)

// SmartSampleConfig is not here
type SmartSampleConfig struct {
	EtcdServer           etcdIntf.Server   `json:"-"`
	EtcdClient           etcdIntf.Client   `json:"-"`
	AdditionalDimensions map[string]string `json:",omitempty"`
	ClusterName          *string           `json:"-"`
	Distributor          *bool             `json:"-"`
	Idx                  int               `json:"-"`
}

// SmartSampler is not here
type SmartSampler struct{}

// DebugEndpoints does nothing
func (f *SmartSampler) DebugEndpoints() map[string]http.Handler {
	return map[string]http.Handler{}
}

// StartupFinished does nothing
func (f *SmartSampler) StartupFinished() error {
	return nil
}

// AddSpans does nothing
func (f *SmartSampler) AddSpans(context context.Context, spans []*trace.Span, sink trace.Sink) error {
	return sink.AddSpans(context, spans)
}

// DebugDatapoints returns datapoints that are used for debugging
func (f *SmartSampler) DebugDatapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{}
}

// DefaultDatapoints returns a set of default datapoints about the sampler
func (f *SmartSampler) DefaultDatapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{}
}

// Datapoints implements the sfxclient.Collector interface and returns all datapoints
func (f *SmartSampler) Datapoints() []*datapoint.Datapoint {
	return append(f.DebugDatapoints(), f.DefaultDatapoints()...)
}

// Close does nothing
func (f *SmartSampler) Close() error {
	return nil
}

type dtsink interface {
	sfxclient.Sink
	trace.Sink
}

// ConfigureHTTPSink does nothing
func (f *SmartSampler) ConfigureHTTPSink(sink *sfxclient.HTTPSink) {
}

// New returns you nothing
func New(*SmartSampleConfig, log.Logger, dtsink) (*SmartSampler, error) {
	return nil, errors.New("you are attempting to configure a regular SignalFx Gateway with the config of a Smart Gateway. This is an unsupported configuration")
}
