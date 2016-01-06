package csv

import (
	"os"

	"sync"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/pointer"
	"golang.org/x/net/context"
)

// Config controls the optional configuration of the csv forwarder
type Config struct {
	Filename    *string
	WriteString func(f *os.File, s string) (ret int, err error)
}

var defaultConfig = &Config{
	Filename:    pointer.String("datapoints.csv"),
	WriteString: func(f *os.File, s string) (ret int, err error) { return f.WriteString(s) },
}

// Forwarder prints datapoints to a file
type Forwarder struct {
	mu          sync.Mutex
	file        *os.File
	writeString func(f *os.File, s string) (ret int, err error)
}

var _ dpsink.Sink = &Forwarder{}

// Datapoints returns nothing and exists to satisfy the protocol.Forwarder interface
func (f *Forwarder) Datapoints() []*datapoint.Datapoint {
	return nil
}

// AddDatapoints writes the points to a file
func (f *Forwarder) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, dp := range points {
		if _, err := f.writeString(f.file, dp.String()+"\n"); err != nil {
			return errors.Annotate(err, "cannot write datapoint to string")
		}
	}
	return f.file.Sync()
}

// AddEvents writes the events to a file
func (f *Forwarder) AddEvents(ctx context.Context, events []*event.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range events {
		if _, err := f.writeString(f.file, e.String()+"\n"); err != nil {
			return errors.Annotate(err, "cannot write event to string")
		}
	}
	return f.file.Sync()
}

// Close the file we write to
func (f *Forwarder) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.file.Close()
}

//// ForwarderLoader loads a CSV forwarder forwarding points from proxy to a file
//func ForwarderLoader(forwardTo *config.ForwardTo) (*Forwarder, error) {
//	structdefaults.FillDefaultFrom(forwardTo, csvDefaultConfig)
//	return NewForwarder(*forwardTo.Name, *forwardTo.Filename)
//}

// NewForwarder creates a new filename forwarder
func NewForwarder(config *Config) (*Forwarder, error) {
	config = pointer.FillDefaultFrom(config, defaultConfig).(*Config)
	file, err := os.OpenFile(*config.Filename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, os.FileMode(0600))
	if err != nil {
		return nil, errors.Annotatef(err, "cannot open file %s", *config.Filename)
	}

	ret := &Forwarder{
		writeString: config.WriteString,
		file:        file,
	}
	return ret, nil
}
