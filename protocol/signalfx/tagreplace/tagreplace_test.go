package tagreplace

import (
	"context"
	"testing"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/pointer"
	"github.com/signalfx/golib/v3/trace"
	. "github.com/smartystreets/goconvey/convey"
)

type end struct {
}

func (e *end) AddSpans(ctx context.Context, spans []*trace.Span) error {
	return nil
}

func (e *end) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	return nil
}

func (e *end) AddEvents(ctx context.Context, events []*event.Event) error {
	return nil
}

func Test(t *testing.T) {
	var cases = []struct {
		desc       string
		rules      []string
		inputSpan  *trace.Span
		outputSpan *trace.Span
		exitEarly  bool
	}{
		{
			desc:       "test single replacement",
			rules:      []string{`^\/api\/v1\/document\/(?P<documentId>.*)\/update$`},
			inputSpan:  &trace.Span{Name: pointer.String("/api/v1/document/321083210/update")},
			outputSpan: &trace.Span{Name: pointer.String("/api/v1/document/{documentId}/update"), Tags: map[string]string{"documentId": "321083210"}},
			exitEarly:  false,
		},
		{
			desc:       "test multi replacement",
			rules:      []string{`^\/api\/(?P<version>.*)\/document\/(?P<documentId>.*)\/update$`},
			inputSpan:  &trace.Span{Name: pointer.String("/api/v1/document/321083210/update")},
			outputSpan: &trace.Span{Name: pointer.String("/api/{version}/document/{documentId}/update"), Tags: map[string]string{"documentId": "321083210", "version": "v1"}},
			exitEarly:  false,
		},
		{
			desc: "test exit early",
			rules: []string{`^\/api\/v1\/document\/(?P<documentId>.*)\/update$`,
				`^\/api\/(?P<version>.*)\/document\/(?P<documentId>.*)\/update$`},
			inputSpan:  &trace.Span{Name: pointer.String("/api/v1/document/321083210/update")},
			outputSpan: &trace.Span{Name: pointer.String("/api/v1/document/{documentId}/update"), Tags: map[string]string{"documentId": "321083210"}},
			exitEarly:  true,
		},
	}
	Convey("test tag replace", t, func() {
		for _, tc := range cases {
			Convey("Testing case: "+tc.desc, func() {
				e := &end{}
				tr, err := New(tc.rules, tc.exitEarly, e)
				So(err, ShouldBeNil)
				So(tr, ShouldNotBeNil)
				err = tr.AddSpans(context.Background(), []*trace.Span{tc.inputSpan})
				So(err, ShouldBeNil)

				So(*tc.inputSpan.Name, ShouldEqual, *tc.outputSpan.Name)
				So(tc.inputSpan.Tags, ShouldResemble, tc.outputSpan.Tags)
			})
		}
	})

}

func Benchmark(b *testing.B) {
	spans := make([]*trace.Span, 0, b.N)
	for i := 0; i < b.N; i++ {
		spans = append(spans, &trace.Span{Name: pointer.String("/api/v1/document/321083210/update"), Tags: map[string]string{}})
	}
	tr, _ := New([]string{
		`^\/api\/(?P<version>.*)\/document\/(?P<documentId>.*)\/update$`,
	}, false, &end{})
	b.ResetTimer()
	b.ReportAllocs()
	tr.AddSpans(context.Background(), spans)
}

func TestBad(t *testing.T) {
	Convey("test bad regex", t, func() {
		_, err := New([]string{`ntId>.*)\/update$`}, false, &end{})
		So(err, ShouldNotBeNil)
	})
	Convey("test no sub exp regex", t, func() {
		_, err := New([]string{`^\/api\/version\/document\/documentId\/update$`}, false, &end{})
		So(err, ShouldNotBeNil)
	})
	Convey("test non named sub exp regex", t, func() {
		_, err := New([]string{`^\/api\/version\/document\/(.*)\/update$`}, false, &end{})
		So(err, ShouldNotBeNil)
	})
	Convey("test passthroughs", t, func() {
		tr := &TagReplace{next: &end{}}
		So(tr.AddDatapoints(context.Background(), []*datapoint.Datapoint{}), ShouldBeNil)
		So(tr.AddEvents(context.Background(), []*event.Event{}), ShouldBeNil)
	})
}
