package dpsink

import (
	"github.com/signalfx/metricproxy/datapoint"
	"golang.org/x/net/context"
)

// A Sink is an object that can accept datapoints and do something with them, like forward them
// to some endpoint
type Sink interface {
	AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error
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
