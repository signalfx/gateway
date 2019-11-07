package spanobfuscation

import (
	"context"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/datapoint/dpsink"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/trace"
)

type rmsink interface {
	dpsink.Sink
	trace.Sink
}

var _ rmsink = &SpanTagRemoval{}

// SpanTagRemoval does a wildcard search for service and operation name, and removes the given tags from matching spans
// This modifies the objects parsed, so in a concurrent context, you will want to copy the objects sent here first
type SpanTagRemoval struct {
	rules []*rule
	next  rmsink
}

//AddDatapoints is a passthrough
func (o *SpanTagRemoval) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	return o.next.AddDatapoints(ctx, points)
}

//AddEvents is a passthrough
func (o *SpanTagRemoval) AddEvents(ctx context.Context, events []*event.Event) error {
	return o.next.AddEvents(ctx, events)
}

// AddSpans maps all rules to all spans and deletes the specified tags if the service and operation match.
// This can be very expensive, and modifies the spans
func (o *SpanTagRemoval) AddSpans(ctx context.Context, spans []*trace.Span) error {
	for _, s := range spans {
		service := ""
		name := ""
		if s.LocalEndpoint != nil && s.LocalEndpoint.ServiceName != nil {
			service = *s.LocalEndpoint.ServiceName
		}
		if s.Name != nil {
			name = *s.Name
		}

		for _, r := range o.rules {
			if r.service.Match(service) && r.operation.Match(name) {
				for _, t := range r.tags {
					delete(s.Tags, t)
				}
			}
		}
	}
	return o.next.AddSpans(ctx, spans)
}

// NewRm returns you a new SpanTagRemoval object
func NewRm(ruleConfigs []*TagMatchRuleConfig, next rmsink) (*SpanTagRemoval, error) {
	rules, err := getRules(ruleConfigs)
	if err != nil {
		return nil, err
	}

	return &SpanTagRemoval{
		rules: rules,
		next:  next,
	}, nil
}
