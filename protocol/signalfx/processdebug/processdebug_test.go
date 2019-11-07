package processdebug

import (
	"context"
	"testing"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/pointer"
	"github.com/signalfx/golib/v3/trace"
	"github.com/smartystreets/assertions"
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
		inputSpan  *trace.Span
		outputSpan *trace.Span
	}{
		{
			desc:       "no debug or sampling priority tag",
			inputSpan:  &trace.Span{},
			outputSpan: &trace.Span{},
		},
		{
			desc:       "debug set with no sampling priority tag",
			inputSpan:  &trace.Span{Debug: pointer.Bool(true)},
			outputSpan: &trace.Span{Debug: pointer.Bool(true), Tags: map[string]string{"sampling.priority": "1"}},
		},
		{
			desc:       "no debug with sampling priority tag set",
			inputSpan:  &trace.Span{Tags: map[string]string{"sampling.priority": "1"}},
			outputSpan: &trace.Span{Debug: pointer.Bool(true), Tags: map[string]string{"sampling.priority": "1"}},
		},
		{
			desc:       "debug set with non-true sampling priority tag",
			inputSpan:  &trace.Span{Debug: pointer.Bool(true), Tags: map[string]string{"sampling.priority": "0"}},
			outputSpan: &trace.Span{Debug: pointer.Bool(true), Tags: map[string]string{"sampling.priority": "1"}},
		},
	}
	Convey("test processDebug", t, func() {
		for _, tc := range cases {
			e := &end{}
			pd := New(e)
			So(pd, assertions.ShouldNotBeNil)
			err := pd.AddSpans(context.Background(), []*trace.Span{tc.inputSpan})
			So(err, ShouldBeNil)

			So(tc.inputSpan.Debug, ShouldResemble, tc.outputSpan.Debug)
			So(tc.inputSpan.Tags, ShouldResemble, tc.outputSpan.Tags)
		}
	})
}

func TestPassthroughs(t *testing.T) {
	Convey("test passthroughs", t, func() {
		at := &ProcessDebug{next: &end{}}
		So(at.AddDatapoints(context.Background(), []*datapoint.Datapoint{}), ShouldBeNil)
		So(at.AddEvents(context.Background(), []*event.Event{}), ShouldBeNil)
	})
}
