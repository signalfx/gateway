package spanobfuscation

import (
	"context"
	"testing"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/pointer"
	"github.com/signalfx/golib/v3/trace"
	. "github.com/smartystreets/goconvey/convey"
)

type rmend struct {
}

func (e *rmend) AddSpans(ctx context.Context, spans []*trace.Span) error {
	return nil
}

func (e *rmend) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	return nil
}

func (e *rmend) AddEvents(ctx context.Context, events []*event.Event) error {
	return nil
}

func TestTagDelete(t *testing.T) {
	Convey("Given a SpanTagRemoval config", t, func() {
		config := []*TagMatchRuleConfig{
			{
				Service: pointer.String("test-service"),
				Tags:    []string{"delete-me"},
			},
			{
				Service:   pointer.String("some*service"),
				Operation: pointer.String("sensitive*"),
				Tags:      []string{"PII", "SSN"},
			},
		}
		so, _ := NewRm(config, &rmend{})
		Convey("should remove tag from exact-match service", func() {
			spans := []*trace.Span{makeSpan("test-service", "shouldn't matter", map[string]string{"delete-me": "val"})}
			so.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{})
		})
		Convey("should not remove tag from exact-match service as prefix", func() {
			spans := []*trace.Span{makeSpan("false-test-service", "shouldn't matter", map[string]string{"delete-me": "val"})}
			so.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{"delete-me": "val"})
		})
		Convey("should not remove tag from exact-match service as suffix", func() {
			spans := []*trace.Span{makeSpan("test-service-extra", "shouldn't matter", map[string]string{"delete-me": "val"})}
			so.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{"delete-me": "val"})
		})
		Convey("should remove tag from matching wildcard service and operation", func() {
			spans := []*trace.Span{makeSpan("some-test-service", "sensitive-data-leak", map[string]string{"PII": "val"})}
			so.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{})
		})
		Convey("should not remove tag with mismatched tag name", func() {
			spans := []*trace.Span{makeSpan("some-test-service", "sensitive-data-leak", map[string]string{"delete-me": "val"})}
			so.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{"delete-me": "val"})
		})
		Convey("should not remove tag with matching service but unmatched operation", func() {
			spans := []*trace.Span{makeSpan("some-test-service", "secure-op", map[string]string{"PII": "val"})}
			so.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{"PII": "val"})
		})
		Convey("should remove all tags defined in the removal rule", func() {
			spans := []*trace.Span{makeSpan("some-test-service", "sensitive-data-leak", map[string]string{"PII": "val", "SSN": "111-22-3333"})}
			so.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{})
		})
		Convey("should handle an empty span", func() {
			spans := []*trace.Span{{}}
			err := so.AddSpans(context.Background(), spans)
			So(err, ShouldBeNil)
		})
		Convey("should handle a span with an empty service", func() {
			spans := []*trace.Span{{LocalEndpoint: &trace.Endpoint{}}}
			err := so.AddSpans(context.Background(), spans)
			So(err, ShouldBeNil)
		})
	})
}

func TestNewRmBad(t *testing.T) {
	Convey("test missing tags", t, func() {
		_, err := NewRm([]*TagMatchRuleConfig{{}}, &rmend{})
		So(err, ShouldNotBeNil)
	})
	Convey("test empty tag name", t, func() {
		_, err := NewRm([]*TagMatchRuleConfig{{Tags: []string{""}}}, &rmend{})
		So(err, ShouldNotBeNil)
	})
	Convey("test empty tags array", t, func() {
		_, err := NewRm([]*TagMatchRuleConfig{{Tags: []string{}}}, &rmend{})
		So(err, ShouldNotBeNil)
	})
}

func TestRmPassthrough(t *testing.T) {
	Convey("test passthroughs", t, func() {
		so := &SpanTagRemoval{next: &rmend{}}
		So(so.AddDatapoints(context.Background(), []*datapoint.Datapoint{}), ShouldBeNil)
		So(so.AddEvents(context.Background(), []*event.Event{}), ShouldBeNil)
	})
}

func BenchmarkRmOne(b *testing.B) {
	spans := make([]*trace.Span, 0, b.N)
	for i := 0; i < b.N; i++ {
		spans = append(spans, makeSpan("some-test-service", "test-op", map[string]string{"PII": "name", "otherTag": "ok"}))
	}
	config := []*TagMatchRuleConfig{
		{
			Service:   pointer.String("some*test*service"),
			Operation: pointer.String("test*"),
			Tags:      []string{"PII"},
		},
	}
	so, _ := NewRm(config, &rmend{})
	b.ResetTimer()
	b.ReportAllocs()
	so.AddSpans(context.Background(), spans)
}

func BenchmarkRmTen(b *testing.B) {
	spans := make([]*trace.Span, 0, b.N)
	for i := 0; i < b.N; i++ {
		spans = append(spans, makeSpan("some-test-service", "test-op", map[string]string{"PII": "name", "otherTag": "ok"}))
	}
	config := make([]*TagMatchRuleConfig, 0, 10)
	for i := 0; i < 9; i++ {
		rule := &TagMatchRuleConfig{
			Service:   pointer.String("some*test*service" + string(i)),
			Operation: pointer.String("test*"),
			Tags:      []string{"PII"},
		}
		config = append(config, rule)
	}
	config = append(config, &TagMatchRuleConfig{
		Service:   pointer.String("some*test*service"),
		Operation: pointer.String("test*"),
		Tags:      []string{"PII"},
	})
	so, _ := NewRm(config, &rmend{})
	b.ResetTimer()
	b.ReportAllocs()
	so.AddSpans(context.Background(), spans)
}

func makeSpan(service string, operation string, tags map[string]string) *trace.Span {
	localEndpoint := &trace.Endpoint{
		ServiceName: pointer.String(service),
	}
	return &trace.Span{
		Name:          pointer.String(operation),
		LocalEndpoint: localEndpoint,
		Tags:          tags,
	}
}
