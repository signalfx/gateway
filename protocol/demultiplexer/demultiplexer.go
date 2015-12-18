package demultiplexer

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"golang.org/x/net/context"
)

// Demultiplexer is a sink that forwards points it sees to multiple sinks
type Demultiplexer struct {
	sendTo []dpsink.Sink
}

var _ dpsink.Sink = &Demultiplexer{}

// AddDatapoints forwards all points to each sendTo sink.  Returns the error message of the last
// sink to have an error.
func (streamer *Demultiplexer) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	if len(points) == 0 {
		return nil
	}
	var err error
	for _, sendTo := range streamer.sendTo {
		err1 := sendTo.AddDatapoints(ctx, points)
		if err1 != nil {
			err = err1
		}
	}
	return err
}

// AddEvents forwards all events to each sendTo sink.  Returns the error message of the last
// sink to have an error.
func (streamer *Demultiplexer) AddEvents(ctx context.Context, points []*event.Event) error {
	if len(points) == 0 {
		return nil
	}
	var err error
	for _, sendTo := range streamer.sendTo {
		err1 := sendTo.AddEvents(ctx, points)
		if err1 != nil {
			err = err1
		}
	}
	return err
}

// New creates a new forwarder that sends datapoints to multiple recievers
func New(sendTo []dpsink.Sink) *Demultiplexer {
	ret := &Demultiplexer{
		sendTo: make([]dpsink.Sink, len(sendTo)),
	}
	for i := range sendTo {
		ret.sendTo[i] = dpsink.FromChain(sendTo[i], dpsink.NextWrap(&dpsink.RateLimitErrorLogging{
			LogThrottle: time.Second,
			Callback:    dpsink.LogCallback("Error forwarding points", log.StandardLogger()),
		}))
	}
	return ret
}
