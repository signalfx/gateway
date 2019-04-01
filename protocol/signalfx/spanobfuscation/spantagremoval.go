package spanobfuscation

import (
	"context"
	"fmt"
	"strings"

	"github.com/gobwas/glob"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/trace"
)

type sink interface {
	dpsink.Sink
	trace.Sink
}

var _ sink = &SpanTagRemoval{}

// SpanTagRemoval does a wildcard search for service and operation name, and removes the given tags from matching spans
// This modifies the objects parsed, so in a concurrent context, you will want to copy the objects sent here first
type SpanTagRemoval struct {
	rules []*rule
	next  sink
}

type rule struct {
	service   glob.Glob
	operation glob.Glob
	tags      []string
}

// TagRemovalRuleConfig describes a wildcard search for a service and operation, along with which tags to remove on the
// matching spans
// Service and Operation can both include "*" for wildcard search, but TagName will only perform an exact text match
// If Service or Operation are omitted, they will use a default value of "*", to match any service/operation
// TagName must be present, and cannot be an empty string
type TagRemovalRuleConfig struct {
	Service   *string  `json:",omitempty"`
	Operation *string  `json:",omitempty"`
	Tags      []string `json:",omitempty"`
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

// New returns you a new SpanTagRemoval object
func New(ruleConfigs []*TagRemovalRuleConfig, next sink) (*SpanTagRemoval, error) {
	var rules []*rule
	for _, r := range ruleConfigs {
		service := "*"
		if r.Service != nil {
			service = *r.Service
		}
		operation := "*"
		if r.Operation != nil {
			operation = *r.Operation
		}
		if len(r.Tags) == 0 {
			return nil, fmt.Errorf("must include Tags for %s:%s", service, operation)
		}

		serviceGlob := getGlob(service)
		operationGlob := getGlob(operation)
		for _, t := range r.Tags {
			if t == "" {
				return nil, fmt.Errorf("found empty tag in %s:%s", service, operation)
			}
		}

		rules = append(rules,
			&rule{
				service:   serviceGlob,
				operation: operationGlob,
				tags:      r.Tags,
			})
	}

	return &SpanTagRemoval{
		rules: rules,
		next:  next,
	}, nil
}

func getGlob(pattern string) glob.Glob {
	patternParts := strings.Split(pattern, "*")
	for i := 0; i < len(patternParts); i++ {
		patternParts[i] = glob.QuoteMeta(patternParts[i])
	}
	return glob.MustCompile(strings.Join(patternParts, "*"))
}
