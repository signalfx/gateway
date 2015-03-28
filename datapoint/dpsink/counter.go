package dpsink

import (
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/signalfx/metricproxy/datapoint"
	"golang.org/x/net/context"
)

// Counter records stats on datapoints to go trhough it as a sink middleware
type Counter struct {
	TotalProcessErrors int64
	TotalDatapoints    int64
	TotalProcessCalls  int64
	ProcessErrorPoints int64
	TotalProcessTimeNs int64
	CallsInFlight      int64
}

type counterSink struct {
	parent    *Counter
	forwardTo Sink
}

// Stats related to this c, including errors processing datapoints
func (c *Counter) Stats(dimensions map[string]string) []*datapoint.Datapoint {
	ret := make([]*datapoint.Datapoint, 0, 6)

	ret = append(ret, datapoint.NewOnHostDatapointDimensions(
		"total_process_errors",
		datapoint.NewIntValue(atomic.LoadInt64(&c.TotalProcessErrors)),
		datapoint.Counter,
		dimensions))

	ret = append(ret, datapoint.NewOnHostDatapointDimensions(
		"total_datapoints",
		datapoint.NewIntValue(atomic.LoadInt64(&c.TotalDatapoints)),
		datapoint.Counter,
		dimensions))

	ret = append(ret, datapoint.NewOnHostDatapointDimensions(
		"total_process_calls",
		datapoint.NewIntValue(atomic.LoadInt64(&c.TotalProcessCalls)),
		datapoint.Counter,
		dimensions))

	ret = append(ret, datapoint.NewOnHostDatapointDimensions(
		"dropped_points",
		datapoint.NewIntValue(atomic.LoadInt64(&c.ProcessErrorPoints)),
		datapoint.Counter,
		dimensions))

	ret = append(ret, datapoint.NewOnHostDatapointDimensions(
		"process_time_ns",
		datapoint.NewIntValue(atomic.LoadInt64(&c.TotalProcessTimeNs)),
		datapoint.Counter,
		dimensions))

	ret = append(ret, datapoint.NewOnHostDatapointDimensions(
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
		log.WithField("err", err).Warn("Unable to process datapoints")
	}
	return err
}

// SinkMiddleware creates a sink that forwards points to the parameter sink
func (c *Counter) SinkMiddleware(sendTo Sink) Sink {
	return &counterSink{
		parent:    c,
		forwardTo: sendTo,
	}
}

func (c *counterSink) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	return c.parent.AddDatapoints(ctx, points, c.forwardTo)
}
