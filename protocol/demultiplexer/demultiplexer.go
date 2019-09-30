package demultiplexer

import (
	"context"
	"github.com/signalfx/gateway/logkey"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/trace"
	"sync/atomic"
	"time"
)

// Demultiplexer is a sink that forwards points it sees to multiple sinks
type Demultiplexer struct {
	DatapointSinks []dpsink.DSink
	EventSinks     []dpsink.ESink
	TraceSinks     []trace.Sink
	Logger         log.Logger
	LateDuration   *time.Duration
	FutureDuration *time.Duration
	stats          struct {
		lateDps      int64
		futureDps    int64
		lateEvents   int64
		futureEvents int64
		lateSpans    int64
		futureSpans  int64
	}
}

var _ dpsink.Sink = &Demultiplexer{}

// AddDatapoints forwards all points to each sendTo sink.  Returns the error message of the last
// sink to have an error.
func (streamer *Demultiplexer) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	if len(points) == 0 {
		return nil
	}
	streamer.handleLateOrFuturePoints(points)
	if v := ctx.Value(sfxclient.TokenHeaderName); v != nil {
		for _, d := range points {
			d.Meta[sfxclient.TokenHeaderName] = v
		}
	}
	var errs []error
	for _, sendTo := range streamer.DatapointSinks {
		if err := sendTo.AddDatapoints(ctx, points); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.NewMultiErr(errs)
}

func (streamer *Demultiplexer) handleLateOrFuturePoints(points []*datapoint.Datapoint) {
	if streamer.FutureDuration != nil || streamer.LateDuration != nil {
		now := time.Now()

		for _, d := range points {
			if streamer.FutureDuration != nil && d.Timestamp.After(now.Add(*streamer.FutureDuration)) {
				atomic.AddInt64(&streamer.stats.futureDps, 1)
				streamer.Logger.Log(logkey.Name, d.String(), logkey.Delta, d.Timestamp.Sub(now), "datapoint received too far into the future")
			} else if streamer.LateDuration != nil && d.Timestamp.Before(now.Add(-*streamer.LateDuration)) {
				atomic.AddInt64(&streamer.stats.lateDps, 1)
				streamer.Logger.Log(logkey.Name, d.String(), logkey.Delta, now.Sub(d.Timestamp), "datapoint received too far into the past")
			}
		}
	}
}

// AddEvents forwards all events to each sendTo sink.  Returns the error message of the last
// sink to have an error.
func (streamer *Demultiplexer) AddEvents(ctx context.Context, events []*event.Event) error {
	if len(events) == 0 {
		return nil
	}
	streamer.handleLateOrFutureEvents(events)
	if v := ctx.Value(sfxclient.TokenHeaderName); v != nil {
		for _, e := range events {
			addMeta(&e.Meta, sfxclient.TokenHeaderName, v)
		}
	}
	var errs []error
	for _, sendTo := range streamer.EventSinks {
		if err := sendTo.AddEvents(ctx, events); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.NewMultiErr(errs)
}

func (streamer *Demultiplexer) handleLateOrFutureEvents(events []*event.Event) {
	if streamer.FutureDuration != nil || streamer.LateDuration != nil {
		now := time.Now()

		for _, d := range events {
			if streamer.FutureDuration != nil && d.Timestamp.After(now.Add(*streamer.FutureDuration)) {
				atomic.AddInt64(&streamer.stats.futureEvents, 1)
				streamer.Logger.Log(logkey.Name, d.String(), logkey.Delta, d.Timestamp.Sub(now), "event received too far into the future")
			} else if streamer.LateDuration != nil && d.Timestamp.Before(now.Add(-*streamer.LateDuration)) {
				atomic.AddInt64(&streamer.stats.lateEvents, 1)
				streamer.Logger.Log(logkey.Name, d.String(), logkey.Delta, now.Sub(d.Timestamp), "event received too far into the past")
			}
		}
	}
}

func addMeta(m *map[interface{}]interface{}, k interface{}, v interface{}) {
	if *m == nil {
		*m = make(map[interface{}]interface{})
	}
	(*m)[k] = v
}

// AddSpans forwards all traces to each sentTo sink. Returns the error of the last sink to have an error.
// to avoid conflicts with adding tags in forwarders, each span needs to be a copy to avoid concurrent modification issues
func (streamer *Demultiplexer) AddSpans(ctx context.Context, spans []*trace.Span) error {
	if len(spans) == 0 {
		return nil
	}
	streamer.handleLateOrFutureSpans(spans)
	if v := ctx.Value(sfxclient.TokenHeaderName); v != nil {
		for _, s := range spans {
			addMeta(&s.Meta, sfxclient.TokenHeaderName, v)
		}
	}
	var errs []error
	for i, sendTo := range streamer.TraceSinks {
		toSend := spans
		if i < len(streamer.TraceSinks)-1 {
			// this is because of smart samplers
			toSend = deepCopySpans(spans)
		}
		if err := sendTo.AddSpans(ctx, toSend); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.NewMultiErr(errs)
}

func deepCopySpans(spans []*trace.Span) []*trace.Span {
	retSpans := make([]*trace.Span, len(spans))
	for i, s := range spans {
		tags := make(map[string]string, len(s.Tags))
		for k, v := range s.Tags {
			tags[k] = v
		}
		retSpans[i] = &trace.Span{
			TraceID:        s.TraceID,
			Name:           s.Name,
			ParentID:       s.ParentID,
			ID:             s.ID,
			Kind:           s.Kind,
			Timestamp:      s.Timestamp,
			Duration:       s.Duration,
			Debug:          s.Debug,
			Shared:         s.Shared,
			LocalEndpoint:  s.LocalEndpoint,
			RemoteEndpoint: s.RemoteEndpoint,
			Annotations:    s.Annotations,
			Tags:           tags,
		}
	}
	return retSpans
}

func (streamer *Demultiplexer) handleLateOrFutureSpans(spans []*trace.Span) {
	if streamer.FutureDuration != nil || streamer.LateDuration != nil {
		now := time.Now()
		for _, d := range spans {
			if d.Timestamp != nil {
				if streamer.FutureDuration != nil && time.Unix(0, *d.Timestamp*int64(time.Microsecond)).After(now.Add(*streamer.FutureDuration)) {

					atomic.AddInt64(&streamer.stats.futureSpans, 1)
					streamer.Logger.Log(logkey.Name, d.ID, logkey.Delta, time.Unix(0, *d.Timestamp*int64(time.Microsecond)).Sub(now), "trace received too far into the future")
				} else if streamer.LateDuration != nil && time.Unix(0, *d.Timestamp*int64(time.Microsecond)).Before(now.Add(-*streamer.LateDuration)) {

					atomic.AddInt64(&streamer.stats.lateSpans, 1)
					streamer.Logger.Log(logkey.Name, d.ID, logkey.Delta, now.Sub(time.Unix(0, *d.Timestamp*int64(time.Microsecond))), "trace received too far into the past")
				}
			}
		}
	}
}

// Datapoints adheres to the sfxclient.Collector interface
func (streamer *Demultiplexer) Datapoints() []*datapoint.Datapoint {
	var dps []*datapoint.Datapoint
	if streamer.FutureDuration != nil {
		dps = append(dps, []*datapoint.Datapoint{
			sfxclient.Cumulative("future.count", map[string]string{"type": "datapoint"}, atomic.LoadInt64(&streamer.stats.futureDps)),
			sfxclient.Cumulative("future.count", map[string]string{"type": "event"}, atomic.LoadInt64(&streamer.stats.futureEvents)),
			sfxclient.Cumulative("future.count", map[string]string{"type": "spans"}, atomic.LoadInt64(&streamer.stats.futureSpans)),
		}...)
	}
	if streamer.LateDuration != nil {
		dps = append(dps, []*datapoint.Datapoint{
			sfxclient.Cumulative("late.count", map[string]string{"type": "datapoint"}, atomic.LoadInt64(&streamer.stats.lateDps)),
			sfxclient.Cumulative("late.count", map[string]string{"type": "event"}, atomic.LoadInt64(&streamer.stats.lateEvents)),
			sfxclient.Cumulative("late.count", map[string]string{"type": "spans"}, atomic.LoadInt64(&streamer.stats.lateSpans)),
		}...)
	}
	return dps
}
