package dpsink

import (
	"testing"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"golang.org/x/net/context"
)

type expect struct {
	count     int
	forwardTo Sink
}

func (e *expect) AddEvents(ctx context.Context, events []*event.Event) error {
	if len(events) != e.count {
		panic("NOPE")
	}
	if e.forwardTo != nil {
		events = append(events, nil)
		e.forwardTo.AddEvents(ctx, events)
	}
	return nil
}

func (e *expect) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	if len(points) != e.count {
		panic("NOPE")
	}
	if e.forwardTo != nil {
		points = append(points, nil)
		e.forwardTo.AddDatapoints(ctx, points)
	}
	return nil
}

func (e *expect) next(sendTo Sink) Sink {
	return &expect{
		count:     e.count,
		forwardTo: sendTo,
	}
}

func TestFromChain(t *testing.T) {
	e2 := expect{count: 2}
	e1 := expect{count: 1}
	e0 := expect{count: 0}

	chain := FromChain(&e2, e0.next, e1.next)
	chain.AddDatapoints(nil, []*datapoint.Datapoint{})
	chain.AddEvents(nil, []*event.Event{})
}
