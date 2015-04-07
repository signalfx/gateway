package demultiplexer

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/datapoint/dpsink"
	"golang.org/x/net/context"
)

// Demultiplexer is a sink that forwards points it sees to multiple sinks
type Demultiplexer struct {
	sendTo []dpsink.Sink
	name   string
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

// New creates a new forwarder that sends datapoints to multiple recievers
func New(sendTo []dpsink.Sink) *Demultiplexer {
	ret := &Demultiplexer{
		sendTo: make([]dpsink.Sink, len(sendTo)),
		name:   "demultiplexer",
	}
	for i := range sendTo {
		ret.sendTo[i] = dpsink.FromChain(sendTo[i], dpsink.NextWrap(&dpsink.RateLimitErrorLogging{
			LogThrottle: time.Second,
			Callback:    dpsink.LogCallback("Error forwarding points", logrus.StandardLogger()),
		}))
	}
	return ret
}
