package additionalspantags

import (
	"context"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/datapoint/dpsink"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/trace"
)

type sink interface {
	dpsink.Sink
	trace.Sink
}

var _ sink = &AdditionalSpanTags{}

// AdditionalSpanTags adds tags to every span it processes
type AdditionalSpanTags struct {
	tags map[string]string
	next sink
}

// AddDatapoints is a passthrough
func (a *AdditionalSpanTags) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	return a.next.AddDatapoints(ctx, points)
}

// AddEvents is a passthrough
func (a *AdditionalSpanTags) AddEvents(ctx context.Context, events []*event.Event) error {
	return a.next.AddEvents(ctx, events)
}

// AddSpans adds tags to all of the spans that are given
func (a *AdditionalSpanTags) AddSpans(ctx context.Context, spans []*trace.Span) error {
	for _, s := range spans {
		if s.Tags == nil {
			s.Tags = make(map[string]string, len(a.tags))
		}

		for k, v := range a.tags {
			s.Tags[k] = v
		}
	}
	return a.next.AddSpans(ctx, spans)
}

// New returns a new AdditionsSpanTags object
func New(tags map[string]string, next sink) *AdditionalSpanTags {
	return &AdditionalSpanTags{
		tags: tags,
		next: next,
	}
}
