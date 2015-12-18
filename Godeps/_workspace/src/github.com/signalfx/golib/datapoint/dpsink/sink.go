package dpsink

import (
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"golang.org/x/net/context"
)

// A DSink is an object that can accept datapoints and do something with them, like forward them
// to some endpoint
type DSink interface {
	AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error
}

// A ESink is an object that can accept events and do something with them, like forward them
// to some endpoint
type ESink interface {
	AddEvents(ctx context.Context, events []*event.Event) error
}

// A Sink is an object that can accept datapoints or events and do something with them, like forward them
// to some endpoint
type Sink interface {
	DSink
	ESink
}

// NextSink is a special case of a sink that forwards to another sink
type NextSink interface {
	AddDatapoints(ctx context.Context, points []*datapoint.Datapoint, next Sink) error
	AddEvents(ctx context.Context, events []*event.Event, next Sink) error
}

// A MiddlewareConstructor is used by FromChain to chain together a bunch of sinks that forward
// to each other
type MiddlewareConstructor func(sendTo Sink) Sink

// FromChain creates an endpoint Sink that sends calls between multiple middlewares for things like
// counting points in between.
func FromChain(endSink Sink, sinks ...MiddlewareConstructor) Sink {
	for i := len(sinks) - 1; i >= 0; i-- {
		endSink = sinks[i](endSink)
	}
	return endSink
}

type nextWrapped struct {
	forwardTo Sink
	wrapping  NextSink
}

func (n *nextWrapped) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	return n.wrapping.AddDatapoints(ctx, points, n.forwardTo)
}

func (n *nextWrapped) AddEvents(ctx context.Context, events []*event.Event) error {
	return n.wrapping.AddEvents(ctx, events, n.forwardTo)
}

// NextWrap wraps a NextSink to make it usable by MiddlewareConstructor
func NextWrap(wrapping NextSink) MiddlewareConstructor {
	return func(sendTo Sink) Sink {
		return &nextWrapped{
			forwardTo: sendTo,
			wrapping:  wrapping,
		}
	}
}
