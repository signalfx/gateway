package protocol

import (
	"io"

	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/metricproxy/stats"
)

// Forwarder is the basic interface endpoints must support for the proxy to forward to them
type Forwarder interface {
	dpsink.Sink
	stats.Keeper
	io.Closer
}

// CompositeForwarder is a helper struct that expects users to inject their own versions of each
// type
type CompositeForwarder struct {
	dpsink.Sink
	stats.Keeper
	io.Closer
}

var _ Forwarder = &CompositeForwarder{}

// Listener is the basic interface anything that listens for new metrics must implement
type Listener interface {
	stats.Keeper
	io.Closer
}

// ListenerDims are the common stat dimensions we expect on listener protocols
func ListenerDims(name string, typ string) map[string]string {
	return map[string]string{
		"location": "listener",
		"name":     name,
		"type":     typ,
	}
}

// ForwarderDims are the common stat dimensions we expect on forwarder protocols
func ForwarderDims(name string, typ string) map[string]string {
	return map[string]string{
		"location": "forwarder",
		"name":     name,
		"type":     typ,
	}
}

// CompositeListener is a helper struct that expects users to inject their own versions of each
// type
type CompositeListener struct {
	stats.Keeper
	io.Closer
}

var _ Listener = &CompositeListener{}

type compositeCloser []io.Closer

// CompositeCloser creates a new io.Closer whos Close() method calls close of each of the closers
func CompositeCloser(closers ...io.Closer) io.Closer {
	return compositeCloser(closers)
}

func (c compositeCloser) Close() error {
	var e error
	for _, closable := range c {
		err := closable.Close()
		if err != nil {
			e = err
		}
	}
	return e
}

// OkCloser allows any function to become a io.Closer that returns nil and calls itself on close
type OkCloser func()

// Close calls the wrapped function and returns nil
func (o OkCloser) Close() error {
	o()
	return nil
}
