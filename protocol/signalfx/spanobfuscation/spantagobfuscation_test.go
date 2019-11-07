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

type obfend struct {
}

func (e *obfend) AddSpans(ctx context.Context, spans []*trace.Span) error {
	return nil
}

func (e *obfend) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	return nil
}

func (e *obfend) AddEvents(ctx context.Context, events []*event.Event) error {
	return nil
}

func TestObfuscate(t *testing.T) {
	Convey("Given a SpanTagRemoval config", t, func() {
		config := []*TagMatchRuleConfig{
			{
				Service: pointer.String("test-service"),
				Tags:    []string{"obfuscate-me"},
			},
			{
				Service:   pointer.String("some*service"),
				Operation: pointer.String("sensitive*"),
				Tags:      []string{"PII", "SSN"},
			},
		}
		obf, _ := NewObf(config, &obfend{})
		Convey("should obfuscate tag from exact-match service", func() {
			spans := []*trace.Span{makeSpan("test-service", "shouldn't matter", map[string]string{"obfuscate-me": "val"})}
			obf.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{"obfuscate-me": OBFUSCATED})
		})
		Convey("should not obfuscate tag from exact-match service as prefix", func() {
			spans := []*trace.Span{makeSpan("false-test-service", "shouldn't matter", map[string]string{"obfuscate-me": "val"})}
			obf.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{"obfuscate-me": "val"})
		})
		Convey("should not obfuscate tag from exact-match service as suffix", func() {
			spans := []*trace.Span{makeSpan("test-service-extra", "shouldn't matter", map[string]string{"obfuscate-me": "val"})}
			obf.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{"obfuscate-me": "val"})
		})
		Convey("should obfuscate tag from matching wildcard service and operation", func() {
			spans := []*trace.Span{makeSpan("some-test-service", "sensitive-data-leak", map[string]string{"PII": "val"})}
			obf.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{"PII": OBFUSCATED})
		})
		Convey("should not obfuscate tag with mismatched tag name", func() {
			spans := []*trace.Span{makeSpan("some-test-service", "sensitive-data-leak", map[string]string{"obfuscate-me": "val"})}
			obf.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{"obfuscate-me": "val"})
		})
		Convey("should not obfuscate tag with matching service but unmatched operation", func() {
			spans := []*trace.Span{makeSpan("some-test-service", "secure-op", map[string]string{"PII": "val"})}
			obf.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{"PII": "val"})
		})
		Convey("should obfuscate all tags defined in the removal rule", func() {
			spans := []*trace.Span{makeSpan("some-test-service", "sensitive-data-leak", map[string]string{"PII": "val", "SSN": "111-22-3333"})}
			obf.AddSpans(context.Background(), spans)
			So(spans[0].Tags, ShouldResemble, map[string]string{"PII": OBFUSCATED, "SSN": OBFUSCATED})
		})
		Convey("should handle an empty span", func() {
			spans := []*trace.Span{{}}
			err := obf.AddSpans(context.Background(), spans)
			So(err, ShouldBeNil)
		})
		Convey("should handle a span with an empty service", func() {
			spans := []*trace.Span{{LocalEndpoint: &trace.Endpoint{}}}
			err := obf.AddSpans(context.Background(), spans)
			So(err, ShouldBeNil)
		})
	})
}

func TestNewObfBad(t *testing.T) {
	Convey("test missing tags", t, func() {
		_, err := NewObf([]*TagMatchRuleConfig{{}}, &obfend{})
		So(err, ShouldNotBeNil)
	})
	Convey("test empty tag name", t, func() {
		_, err := NewObf([]*TagMatchRuleConfig{{Tags: []string{""}}}, &obfend{})
		So(err, ShouldNotBeNil)
	})
	Convey("test empty tags array", t, func() {
		_, err := NewObf([]*TagMatchRuleConfig{{Tags: []string{}}}, &obfend{})
		So(err, ShouldNotBeNil)
	})
}

func TestObfPassthrough(t *testing.T) {
	Convey("test passthroughs", t, func() {
		obf := &SpanTagObfuscation{next: &obfend{}}
		So(obf.AddDatapoints(context.Background(), []*datapoint.Datapoint{}), ShouldBeNil)
		So(obf.AddEvents(context.Background(), []*event.Event{}), ShouldBeNil)
	})
}

func BenchmarkObfOne(b *testing.B) {
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
	obf, _ := NewObf(config, &rmend{})
	b.ResetTimer()
	b.ReportAllocs()
	obf.AddSpans(context.Background(), spans)
}

func BenchmarkObfTen(b *testing.B) {
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
	obf, _ := NewObf(config, &rmend{})
	b.ResetTimer()
	b.ReportAllocs()
	obf.AddSpans(context.Background(), spans)
}
