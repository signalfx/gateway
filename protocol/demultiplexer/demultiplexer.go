package demultiplexer

import (
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"golang.org/x/net/context"
	"github.com/signalfx/golib/errors"
)

// Demultiplexer is a sink that forwards points it sees to multiple sinks
type Demultiplexer struct {
	DatapointSinks []dpsink.DSink
	EventSinks []dpsink.ESink
}

var _ dpsink.Sink = &Demultiplexer{}

// AddDatapoints forwards all points to each sendTo sink.  Returns the error message of the last
// sink to have an error.
func (streamer *Demultiplexer) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	if len(points) == 0 {
		return nil
	}
	var errs []error
	for _, sendTo := range streamer.DatapointSinks {
		if err := sendTo.AddDatapoints(ctx, points); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.NewMultiErr(errs)
}

// AddEvents forwards all events to each sendTo sink.  Returns the error message of the last
// sink to have an error.
func (streamer *Demultiplexer) AddEvents(ctx context.Context, points []*event.Event) error {
	if len(points) == 0 {
		return nil
	}
	var errs []error
	for _, sendTo := range streamer.EventSinks {
		if err := sendTo.AddEvents(ctx, points); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.NewMultiErr(errs)
}

// TODO: Make sure rate limit error is common in a layer above me
//// New creates a new forwarder that sends datapoints to multiple recievers
//func New(sendTo []dpsink.Sink, logger log.Logger) *Demultiplexer {
//	ret := &Demultiplexer{
//		sendTo: make([]dpsink.Sink, len(sendTo)),
//	}
//	for i := range sendTo {
//		ret.sendTo[i] = dpsink.FromChain(sendTo[i], dpsink.NextWrap(&dpsink.RateLimitErrorLogging{
//			LogThrottle: time.Second,
//			Logger:      log.NewContext(logger).With(logkey.Struct, "demultiplexer"),
//		}))
//	}
//	return ret
//}
