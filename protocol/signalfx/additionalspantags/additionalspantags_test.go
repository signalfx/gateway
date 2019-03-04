package additionalspantags

import (
	"context"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/trace"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
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
		tags       map[string]string
		inputSpan  *trace.Span
		outputSpan *trace.Span
	}{
		{
			desc: "test add single tag",
			tags: map[string]string{
				"tagKey": "tagValue",
			},
			inputSpan:  &trace.Span{Tags: map[string]string{}},
			outputSpan: &trace.Span{Tags: map[string]string{"tagKey": "tagValue"}},
		},
		{
			desc: "test add single tag to nil Tags",
			tags: map[string]string{
				"tagKey": "tagValue",
			},
			inputSpan:  &trace.Span{},
			outputSpan: &trace.Span{Tags: map[string]string{"tagKey": "tagValue"}},
		},
		{
			desc: "test add multiple tags",
			tags: map[string]string{
				"tagKey":    "tagValue",
				"secondTag": "secondValue",
			},
			inputSpan:  &trace.Span{},
			outputSpan: &trace.Span{Tags: map[string]string{"tagKey": "tagValue", "secondTag": "secondValue"}},
		},
		{
			desc: "test add single tag to already existing Tags",
			tags: map[string]string{
				"tagKey": "tagValue",
			},
			inputSpan:  &trace.Span{Tags: map[string]string{"existingKey": "existingValue"}},
			outputSpan: &trace.Span{Tags: map[string]string{"existingKey": "existingValue", "tagKey": "tagValue"}},
		},
		{
			desc: "test overwrite existing tag",
			tags: map[string]string{
				"tagKey": "tagValue",
			},
			inputSpan:  &trace.Span{Tags: map[string]string{"tagKey": "wrongValue"}},
			outputSpan: &trace.Span{Tags: map[string]string{"tagKey": "tagValue"}},
		},
	}
	Convey("test additional tags", t, func() {
		for _, tc := range cases {
			e := &end{}
			at := New(tc.tags, e)
			So(at, ShouldNotBeNil)
			err := at.AddSpans(context.Background(), []*trace.Span{tc.inputSpan})
			So(err, ShouldBeNil)

			So(tc.inputSpan.Tags, ShouldResemble, tc.outputSpan.Tags)
		}
	})
}

func TestPassthroughs(t *testing.T) {
	Convey("test passthroughs", t, func() {
		at := &AdditionalSpanTags{next: &end{}}
		So(at.AddDatapoints(context.Background(), []*datapoint.Datapoint{}), ShouldBeNil)
		So(at.AddEvents(context.Background(), []*event.Event{}), ShouldBeNil)
	})
}
