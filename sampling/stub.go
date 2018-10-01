package sampling

import (
	"context"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/trace"
)

// SmartSampleConfig is not here
type SmartSampleConfig struct{}

// SmartSampler is not here
type SmartSampler struct{}

// AddSpans does nothing
func (forwarder *SmartSampler) AddSpans(context context.Context, spans []*trace.Span, sink trace.Sink) error {
	return nil
}

// Close does nothing
func (forwarder *SmartSampler) Close() error {
	return nil
}

type dtsink interface {
	sfxclient.Sink
	trace.Sink
}

// New returns you nothing
func New(*SmartSampleConfig, log.Logger, dtsink) (*SmartSampler, error) {
	return nil, nil
}
