package dpsink

import (
	"sync/atomic"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dplocal"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"golang.org/x/net/context"
)

// DefaultLogger is used by package structs that don't have a default logger set.
var DefaultLogger = log.DefaultLogger.CreateChild()

// Counter records stats on datapoints to go through it as a sink middleware
type Counter struct {
	TotalProcessErrors int64
	TotalDatapoints    int64
	TotalEvents        int64
	TotalProcessCalls  int64
	ProcessErrorPoints int64
	TotalProcessTimeNs int64
	CallsInFlight      int64
	Logger             log.Logger
}

// Stats related to this c, including errors processing datapoints
func (c *Counter) Stats(dimensions map[string]string) []*datapoint.Datapoint {
	ret := make([]*datapoint.Datapoint, 0, 6)

	ret = append(ret, dplocal.NewOnHostDatapointDimensions(
		"total_process_errors",
		datapoint.NewIntValue(atomic.LoadInt64(&c.TotalProcessErrors)),
		datapoint.Counter,
		dimensions))

	ret = append(ret, dplocal.NewOnHostDatapointDimensions(
		"total_datapoints",
		datapoint.NewIntValue(atomic.LoadInt64(&c.TotalDatapoints)),
		datapoint.Counter,
		dimensions))

	ret = append(ret, dplocal.NewOnHostDatapointDimensions(
		"total_events",
		datapoint.NewIntValue(atomic.LoadInt64(&c.TotalEvents)),
		datapoint.Counter,
		dimensions))

	ret = append(ret, dplocal.NewOnHostDatapointDimensions(
		"total_process_calls",
		datapoint.NewIntValue(atomic.LoadInt64(&c.TotalProcessCalls)),
		datapoint.Counter,
		dimensions))

	ret = append(ret, dplocal.NewOnHostDatapointDimensions(
		"dropped_points",
		datapoint.NewIntValue(atomic.LoadInt64(&c.ProcessErrorPoints)),
		datapoint.Counter,
		dimensions))

	ret = append(ret, dplocal.NewOnHostDatapointDimensions(
		"process_time_ns",
		datapoint.NewIntValue(atomic.LoadInt64(&c.TotalProcessTimeNs)),
		datapoint.Counter,
		dimensions))

	ret = append(ret, dplocal.NewOnHostDatapointDimensions(
		"calls_in_flight",
		datapoint.NewIntValue(atomic.LoadInt64(&c.CallsInFlight)),
		datapoint.Gauge,
		dimensions))
	return ret
}

// AddDatapoints will send points to the next sink and track points send to the next sink
func (c *Counter) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint, next Sink) error {
	atomic.AddInt64(&c.TotalDatapoints, int64(len(points)))
	atomic.AddInt64(&c.TotalProcessCalls, 1)
	atomic.AddInt64(&c.CallsInFlight, 1)
	start := time.Now()
	err := next.AddDatapoints(ctx, points)
	atomic.AddInt64(&c.TotalProcessTimeNs, time.Since(start).Nanoseconds())
	atomic.AddInt64(&c.CallsInFlight, -1)
	if err != nil {
		atomic.AddInt64(&c.TotalProcessErrors, 1)
		atomic.AddInt64(&c.ProcessErrorPoints, int64(len(points)))
		c.logger().Log(log.Err, err, "Unable to process datapoints")
	}
	return err
}

func (c *Counter) logger() log.Logger {
	if c.Logger == nil {
		return DefaultLogger
	}
	return c.Logger
}

// AddEvents will send events to the next sink and track events sent to the next sink
func (c *Counter) AddEvents(ctx context.Context, events []*event.Event, next Sink) error {
	atomic.AddInt64(&c.TotalEvents, int64(len(events)))
	atomic.AddInt64(&c.TotalProcessCalls, 1)
	atomic.AddInt64(&c.CallsInFlight, 1)
	start := time.Now()
	err := next.AddEvents(ctx, events)
	atomic.AddInt64(&c.TotalProcessTimeNs, time.Since(start).Nanoseconds())
	atomic.AddInt64(&c.CallsInFlight, -1)
	if err != nil {
		atomic.AddInt64(&c.TotalProcessErrors, 1)
		atomic.AddInt64(&c.ProcessErrorPoints, int64(len(events)))
		c.logger().Log(log.Err, err, "Unable to process datapoints")
	}
	return err
}
