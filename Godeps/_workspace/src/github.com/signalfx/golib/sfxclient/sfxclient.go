package sfxclient

import (
	"expvar"

	"github.com/signalfx/golib/datapoint"

	"sync"
	"sync/atomic"
	"time"

	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/timekeeper"
	"golang.org/x/net/context"
)

// DefaultReportingDelay is the default interval between new SignalFx schedulers
const DefaultReportingDelay = time.Second * 20

// DefaultErrorHandler is the default way to handle errors by a scheduler.  It simply prints them to stdout
var DefaultErrorHandler = func(err error) error {
	log.DefaultLogger.Log(log.Err, err, "Unable to handle error")
	return nil
}

// Sink is anything that can receive points collected by a Scheduler
type Sink interface {
	AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) (err error)
}

// Collector is anything scheduler can track that emits points
type Collector interface {
	Datapoints() []*datapoint.Datapoint
}

// HashableCollector is a Collector function that can be inserted into a hashmap
type HashableCollector struct {
	Callback func() []*datapoint.Datapoint
}

// CollectorFunc wraps a function to make it a Collector
func CollectorFunc(callback func() []*datapoint.Datapoint) *HashableCollector {
	return &HashableCollector{
		Callback: callback,
	}
}

// Datapoints calls the wrapped function
func (h *HashableCollector) Datapoints() []*datapoint.Datapoint {
	return h.Callback()
}

var _ Collector = CollectorFunc(nil)

type callbackPair struct {
	callbacks         map[Collector]struct{}
	defaultDimensions map[string]string
	expectedSize      int
}

func (c *callbackPair) getDatapoints(now time.Time, sendZeroTime bool) []*datapoint.Datapoint {
	ret := make([]*datapoint.Datapoint, 0, c.expectedSize)
	for callback := range c.callbacks {
		ret = append(ret, callback.Datapoints()...)
	}
	for _, dp := range ret {
		// It's a bit dangerous to modify the map (we don't know how it was passed in) so
		// make a copy to be safe
		dp.Dimensions = AddMaps(c.defaultDimensions, dp.Dimensions)
		if !sendZeroTime && dp.Timestamp.IsZero() {
			dp.Timestamp = now
		}
	}
	c.expectedSize = len(ret)
	return ret
}

// A Scheduler reports metrics to SignalFx
type Scheduler struct {
	Sink             Sink
	Timer            timekeeper.TimeKeeper
	SendZeroTime     bool
	ErrorHandler     func(error) error
	ReportingDelayNs int64

	callbackMutex      sync.Mutex
	callbackMap        map[string]*callbackPair
	previousDatapoints []*datapoint.Datapoint
	stats              struct {
		scheduledSleepCounts int64
		resetIntervalCounts  int64
	}
}

// NewScheduler creates a default SignalFx scheduler that can report metrics to signalfx at some interval
func NewScheduler() *Scheduler {
	return &Scheduler{
		Sink:             NewHTTPDatapointSink(),
		Timer:            timekeeper.RealTime{},
		ErrorHandler:     DefaultErrorHandler,
		ReportingDelayNs: DefaultReportingDelay.Nanoseconds(),
		callbackMap:      make(map[string]*callbackPair),
	}
}

// Var returns an expvar variable that prints the values of the previously reported datapoints
func (s *Scheduler) Var() expvar.Var {
	return expvar.Func(func() interface{} {
		s.callbackMutex.Lock()
		defer s.callbackMutex.Unlock()
		return s.previousDatapoints
	})
}

func (s *Scheduler) collectDatapoints() []*datapoint.Datapoint {
	ret := make([]*datapoint.Datapoint, 0, len(s.previousDatapoints))
	now := s.Timer.Now()
	for _, p := range s.callbackMap {
		ret = append(ret, p.getDatapoints(now, s.SendZeroTime)...)
	}
	return ret
}

// AddCallback adds a collector to the default group
func (s *Scheduler) AddCallback(db Collector) {
	s.AddGroupedCallback("", db)
}

// DefaultDimensions adds a dimension map that are appended to all metrics in the default group
func (s *Scheduler) DefaultDimensions(dims map[string]string) {
	s.GroupedDefaultDimensions("", dims)
}

// GroupedDefaultDimensions adds default dimensions to a specific group
func (s *Scheduler) GroupedDefaultDimensions(group string, dims map[string]string) {
	s.callbackMutex.Lock()
	defer s.callbackMutex.Unlock()
	subgroup, exists := s.callbackMap[group]
	if !exists {
		subgroup = &callbackPair{
			callbacks:         make(map[Collector]struct{}, 0),
			defaultDimensions: dims,
		}
		s.callbackMap[group] = subgroup
	}
	subgroup.defaultDimensions = dims
}

// AddGroupedCallback adds a collector to a specific group
func (s *Scheduler) AddGroupedCallback(group string, db Collector) {
	s.callbackMutex.Lock()
	defer s.callbackMutex.Unlock()
	subgroup, exists := s.callbackMap[group]
	if !exists {
		subgroup = &callbackPair{
			callbacks:         map[Collector]struct{}{db: {}},
			defaultDimensions: map[string]string{},
		}
		s.callbackMap[group] = subgroup
	}
	subgroup.callbacks[db] = struct{}{}
}

// RemoveCallback removes a collector from the default group
func (s *Scheduler) RemoveCallback(db Collector) {
	s.RemoveGroupedCallback("", db)
}

// RemoveGroupedCallback removes a collector from a specific group
func (s *Scheduler) RemoveGroupedCallback(group string, db Collector) {
	s.callbackMutex.Lock()
	defer s.callbackMutex.Unlock()
	if g, exists := s.callbackMap[group]; exists {
		delete(g.callbacks, db)
		if len(g.callbacks) == 0 {
			delete(s.callbackMap, group)
		}
	}
}

// ReportOnce will report any metrics saved in this reporter to SignalFx
func (s *Scheduler) ReportOnce(ctx context.Context) error {
	datapoints := func() []*datapoint.Datapoint {
		// Only take the mutex here so that we don't hold it during the HTTP call
		s.callbackMutex.Lock()
		defer s.callbackMutex.Unlock()
		datapoints := s.collectDatapoints()
		s.previousDatapoints = datapoints
		return datapoints
	}()
	return s.Sink.AddDatapoints(ctx, datapoints)
}

// ReportingDelay sets the interval metrics are reported to SignalFx
func (s *Scheduler) ReportingDelay(delay time.Duration) {
	atomic.StoreInt64(&s.ReportingDelayNs, delay.Nanoseconds())
}

// Schedule will run until either the ErrorHandler returns an error or the context is canceled.  This is intended to
// be run inside a goroutine.
func (s *Scheduler) Schedule(ctx context.Context) error {
	lastReport := s.Timer.Now()
	for {
		reportingDelay := time.Duration(atomic.LoadInt64(&s.ReportingDelayNs))
		wakeupTime := lastReport.Add(reportingDelay)
		now := s.Timer.Now()
		if now.After(wakeupTime) {
			wakeupTime = now.Add(reportingDelay)
			atomic.AddInt64(&s.stats.resetIntervalCounts, 1)
		}
		sleepTime := wakeupTime.Sub(now)

		atomic.AddInt64(&s.stats.scheduledSleepCounts, 1)
		select {
		case <-ctx.Done():
			return errors.Annotate(ctx.Err(), "context closed")
		case <-s.Timer.After(sleepTime):
			lastReport = s.Timer.Now()
			if err := errors.Annotate(s.ReportOnce(ctx), "failed reporting single metric"); err != nil {
				if err2 := errors.Annotate(s.ErrorHandler(err), "error handler returned an error"); err2 != nil {
					return err2
				}
			}
		}
	}
}
