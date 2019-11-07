package spanobfuscation

import (
	"context"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/datapoint/dpsink"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/trace"
)

type obfsink interface {
	dpsink.Sink
	trace.Sink
}

var _ obfsink = &SpanTagObfuscation{}

// SpanTagObfuscation does a wildcard search for service and operation name, and replaces the given tags from matching spans with the OBFUSCATED const
// This modifies the objects parsed, so in a concurrent context, you will want to copy the objects sent here first
type SpanTagObfuscation struct {
	rules []*rule
	next  obfsink
}

// OBFUSCATED is the value to use for obfuscated tags
const OBFUSCATED = "<obfuscated>"

//AddDatapoints is a passthrough
func (o *SpanTagObfuscation) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	return o.next.AddDatapoints(ctx, points)
}

//AddEvents is a passthrough
func (o *SpanTagObfuscation) AddEvents(ctx context.Context, events []*event.Event) error {
	return o.next.AddEvents(ctx, events)
}

// AddSpans maps all rules to all spans and replaces the specified tags with the OBFUSCATED const if the service and operation match.
// This can be very expensive, and modifies the spans
func (o *SpanTagObfuscation) AddSpans(ctx context.Context, spans []*trace.Span) error {
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
					if _, exists := s.Tags[t]; exists {
						s.Tags[t] = OBFUSCATED
					}
				}
			}
		}
	}
	return o.next.AddSpans(ctx, spans)
}

// NewObf returns you a new SpanTagObfuscation object
func NewObf(ruleConfigs []*TagMatchRuleConfig, next obfsink) (*SpanTagObfuscation, error) {
	rules, err := getRules(ruleConfigs)
	if err != nil {
		return nil, err
	}
	return &SpanTagObfuscation{
		rules: rules,
		next:  next,
	}, nil
}
