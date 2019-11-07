package processdebug

import (
	"context"

	"github.com/opentracing/opentracing-go/ext"
	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/datapoint/dpsink"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/pointer"
	"github.com/signalfx/golib/v3/trace"
)

type sink interface {
	dpsink.Sink
	trace.Sink
}

var _ sink = &ProcessDebug{}

// ProcessDebug checks if a span has debug values set
type ProcessDebug struct {
	next sink
}

var spTagName = string(ext.SamplingPriority)

// AddDatapoints is a passthrough
func (p *ProcessDebug) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	return p.next.AddDatapoints(ctx, points)
}

// AddEvents is a passthrough
func (p *ProcessDebug) AddEvents(ctx context.Context, events []*event.Event) error {
	return p.next.AddEvents(ctx, events)
}

// AddSpans updates the "sampling.priority" span tag and Debug field if either value is set
func (p *ProcessDebug) AddSpans(ctx context.Context, spans []*trace.Span) error {
	for _, s := range spans {
		if s.Debug != nil && *s.Debug {
			if s.Tags == nil {
				s.Tags = map[string]string{}
			}
			s.Tags[spTagName] = "1"
			continue
		}
		if s.Tags != nil && s.Tags[spTagName] == "1" {
			s.Debug = pointer.Bool(true)
		}
	}
	return p.next.AddSpans(ctx, spans)
}

// New returns a new ProcessDebug object
func New(next sink) *ProcessDebug {
	return &ProcessDebug{
		next: next,
	}
}
