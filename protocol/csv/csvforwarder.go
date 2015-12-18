package csv

import (
	"io/ioutil"
	"os"

	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/protocol"
	"golang.org/x/net/context"
)

var csvDefaultConfig = &config.ForwardTo{
	Filename: workarounds.GolangDoesnotAllowPointerToStringLiteral("datapoints.csv"),
	Name:     workarounds.GolangDoesnotAllowPointerToStringLiteral("filename-drainer"),
}

// ForwarderLoader loads a CSV forwarder forwarding points from proxy to a file
func ForwarderLoader(forwardTo *config.ForwardTo) (*FilenameForwarder, error) {
	structdefaults.FillDefaultFrom(forwardTo, csvDefaultConfig)
	log.WithField("forwardTo", forwardTo).Info("Creating CSV using final config")
	return NewForwarder(*forwardTo.Name, *forwardTo.Filename)
}

var _ protocol.Forwarder = &FilenameForwarder{}

// FilenameForwarder prints datapoints to a file
type FilenameForwarder struct {
	writeLock   sync.Mutex
	filename    string
	writeString func(f *os.File, s string) (ret int, err error)
}

var _ dpsink.Sink = &FilenameForwarder{}

// Stats returns an empty list
func (connector *FilenameForwarder) Stats() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{}
}

// AddDatapoints writes the points to a file
func (connector *FilenameForwarder) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	connector.writeLock.Lock()
	defer connector.writeLock.Unlock()
	file, err := os.OpenFile(connector.filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0666))
	if err != nil {
		return err
	}
	defer file.Close()
	for _, dp := range points {
		_, err := connector.writeString(file, dp.String()+"\n")
		if err != nil {
			return err
		}
	}
	return nil
}

// AddEvents writes the events to a file
func (connector *FilenameForwarder) AddEvents(ctx context.Context, events []*event.Event) error {
	connector.writeLock.Lock()
	defer connector.writeLock.Unlock()
	file, err := os.OpenFile(connector.filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0666))
	if err != nil {
		return err
	}
	defer file.Close()
	for _, e := range events {
		_, err := connector.writeString(file, e.String()+"\n")
		if err != nil {
			return err
		}
	}
	return nil
}

// Close does nothing.  We foolishly reopen the file on each AddDatapoints
func (connector *FilenameForwarder) Close() error {
	return nil
}

// NewForwarder creates a new filename forwarder
func NewForwarder(name string, filename string) (*FilenameForwarder, error) {
	ret := &FilenameForwarder{
		writeString: func(f *os.File, s string) (ret int, err error) { return f.WriteString(s) },
		filename:    filename,
	}
	if err := ioutil.WriteFile(filename, []byte{}, os.FileMode(0666)); err != nil {
		log.WithField("filename", filename).Info("Unable to verify write for file")
		return nil, err
	}
	return ret, nil
}
