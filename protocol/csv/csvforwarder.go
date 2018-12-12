package csv

import (
	"os"

	"sync"

	"context"
	"encoding/json"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/trace"
	"github.com/signalfx/metricproxy/protocol/filtering"
	"sync/atomic"
)

// Config controls the optional configuration of the csv forwarder
type Config struct {
	Filters     *filtering.FilterObj
	Filename    *string
	WriteString func(f *os.File, s string) (ret int, err error)
}

var defaultConfig = &Config{
	Filters:     &filtering.FilterObj{},
	Filename:    pointer.String("datapoints.csv"),
	WriteString: func(f *os.File, s string) (ret int, err error) { return f.WriteString(s) },
}

type stats struct {
	totalDatapointsForwarded int64
	totalEventsForwarded     int64
	totalSpansForwarded      int64
}

// Forwarder prints datapoints to a file
type Forwarder struct {
	filtering.FilteredForwarder
	mu          sync.Mutex
	file        *os.File
	writeString func(f *os.File, s string) (ret int, err error)
	stats       stats
}

var _ dpsink.Sink = &Forwarder{}

// StartupFinished can be called if you want to do something after startup is complete
func (f *Forwarder) StartupFinished() error {
	return nil
}

// Datapoints returns nothing and exists to satisfy the protocol.Forwarder interface
func (f *Forwarder) Datapoints() []*datapoint.Datapoint {
	return f.GetFilteredDatapoints()
}

// AddDatapoints writes the points to a file
func (f *Forwarder) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	points = f.FilterDatapoints(points)
	f.mu.Lock()
	defer f.mu.Unlock()
	atomic.AddInt64(&f.stats.totalDatapointsForwarded, int64(len(points)))
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
	atomic.AddInt64(&f.stats.totalEventsForwarded, int64(len(events)))
	for _, e := range events {
		if _, err := f.writeString(f.file, e.String()+"\n"); err != nil {
			return errors.Annotate(err, "cannot write event to string")
		}
	}
	return f.file.Sync()
}

// AddSpans writes the spans to a file
func (f *Forwarder) AddSpans(ctx context.Context, spans []*trace.Span) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	atomic.AddInt64(&f.stats.totalSpansForwarded, int64(len(spans)))
	for _, s := range spans {
		b, err := json.Marshal(s)
		if err == nil {
			_, err = f.writeString(f.file, string(b)+"\n")
		}
		if err != nil {
			return errors.Annotate(err, "cannot write span to string")
		}
	}
	return f.file.Sync()
}

// Pipeline returns 0 because csvforwarder doesn't buffer
func (f *Forwarder) Pipeline() int64 {
	return 0
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
	err = ret.Setup(config.Filters)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
