package dpbuffered

import (
	"sync"
	"sync/atomic"

	"context"
	"fmt"
	"github.com/signalfx/gateway/logkey"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/trace"
	"net/http"
	"runtime"
)

// Config controls BufferedForwarder limits
type Config struct {
	BufferSize         *int64
	MaxTotalDatapoints *int64
	MaxTotalEvents     *int64
	MaxTotalSpans      *int64
	MaxDrainSize       *int64
	NumDrainingThreads *int
	Checker            *dpsink.ItemFlagger
	Cdim               *log.CtxDimensions
	Name               *string
	UseAuthFromRequest *bool
}

func (c *Config) String() string {
	return fmt.Sprintf("Config [Name: %s BufferSize: %d MaxTotalDatapoints: %d MaxTotalEvents: %d MaxTotalSpans: %d MaxDrainSize: %d NumDrainingThreads: %d UseAuthFromRequest: %t", *c.Name, *c.BufferSize, *c.MaxTotalDatapoints, *c.MaxTotalEvents, *c.MaxTotalSpans, *c.MaxDrainSize, *c.NumDrainingThreads, *c.UseAuthFromRequest)
}

// DefaultConfig are default values for buffered forwarders
var DefaultConfig = &Config{
	BufferSize:         pointer.Int64(1000000),
	MaxTotalDatapoints: pointer.Int64(1000000),
	MaxTotalEvents:     pointer.Int64(1000000),
	MaxTotalSpans:      pointer.Int64(1000000),
	MaxDrainSize:       pointer.Int64(30000),
	NumDrainingThreads: pointer.Int(runtime.NumCPU()),
	Name:               pointer.String(""),
	UseAuthFromRequest: pointer.Bool(false),
}

// Sink is a dpsink and trace.sink
type Sink interface {
	dpsink.Sink
	trace.Sink
}

type stats struct {
	totalDatapointsBuffered int64
	totalEventsBuffered     int64
	datapointsInFlight      int64
	eventsInFlight          int64
	totalTracesBuffered     int64
	tracesInFlight          int64
}

// BufferedForwarder abstracts out datapoint buffering.  Points put on its channel are buffered
// and sent in large groups to a waiting sink
type BufferedForwarder struct {
	dpChan                      chan []*datapoint.Datapoint
	eChan                       chan []*event.Event
	tChan                       chan []*trace.Span
	config                      *Config
	stats                       stats
	useAuthFromRequest          bool
	threadsWaitingToDie         sync.WaitGroup
	blockingDrainWaitMutex      sync.Mutex
	blockingEventDrainWaitMutex sync.Mutex
	blockingTraceDrainWaitMutex sync.Mutex
	// these are where we hold stuff that weren't the correct token
	// these are protected by the three mutexes above
	holdDatapoints []*datapoint.Datapoint
	holdEvents     []*event.Event
	holdSpans      []*trace.Span

	logger     log.Logger
	checker    *dpsink.ItemFlagger
	cdim       *log.CtxDimensions
	identifier string

	sendTo         Sink
	stopContext    context.Context
	stopFunc       context.CancelFunc
	closeSender    func() error
	afterStartup   func() error
	debugEndpoints func() map[string]http.Handler
}

// DebugEndpoints returns any configured http handlers and the point they can be reached at
func (forwarder *BufferedForwarder) DebugEndpoints() map[string]http.Handler {
	return forwarder.debugEndpoints()
}

var _ dpsink.Sink = &BufferedForwarder{}

type errDPBufferFull string

func (e errDPBufferFull) Error() string {
	return "Forwarder " + string(e) + " unable to send more datapoints.  Buffer full"
}

// AddDatapoints sends the datapoints to a chan buffer that eventually is flushed in big groups
func (forwarder *BufferedForwarder) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	if forwarder.checker.CtxFlagCheck.HasFlag(ctx) {
		forwarder.cdim.With(ctx, forwarder.logger).Log("Datapoint call recieved in buffered forwarder")
	}
	atomic.AddInt64(&forwarder.stats.totalDatapointsBuffered, int64(len(points)))
	if *forwarder.config.MaxTotalDatapoints <= atomic.LoadInt64(&forwarder.stats.totalDatapointsBuffered) {
		atomic.AddInt64(&forwarder.stats.totalDatapointsBuffered, int64(-len(points)))
		return errDPBufferFull(forwarder.identifier)
	}
	select {
	case forwarder.dpChan <- points:
		return nil
	case <-forwarder.stopContext.Done():
		return forwarder.stopContext.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

type errEBufferFull string

func (e errEBufferFull) Error() string {
	return "Forwarder " + string(e) + " unable to send more events.  Buffer full"
}

// AddEvents sends the events to a chan buffer that eventually is flushed in big groups
func (forwarder *BufferedForwarder) AddEvents(ctx context.Context, events []*event.Event) error {
	if forwarder.checker.CtxFlagCheck.HasFlag(ctx) {
		forwarder.cdim.With(ctx, forwarder.logger).Log("Events call received in buffered forwarder")
	}
	atomic.AddInt64(&forwarder.stats.totalEventsBuffered, int64(len(events)))
	if *forwarder.config.MaxTotalEvents <= atomic.LoadInt64(&forwarder.stats.totalEventsBuffered) {
		atomic.AddInt64(&forwarder.stats.totalEventsBuffered, int64(-len(events)))
		return errEBufferFull(forwarder.identifier)
	}
	select {
	case forwarder.eChan <- events:
		return nil
	case <-forwarder.stopContext.Done():
		return forwarder.stopContext.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

type errTBufferFull string

func (e errTBufferFull) Error() string {
	return "Forwarder " + string(e) + " unable to send more traces.  Buffer full"
}

// AddSpans sends the traces to a chan buffer that traceually is flushed in big groups
func (forwarder *BufferedForwarder) AddSpans(ctx context.Context, traces []*trace.Span) error {
	atomic.AddInt64(&forwarder.stats.totalTracesBuffered, int64(len(traces)))
	if *forwarder.config.MaxTotalSpans <= atomic.LoadInt64(&forwarder.stats.totalTracesBuffered) {
		atomic.AddInt64(&forwarder.stats.totalTracesBuffered, int64(-len(traces)))
		return errTBufferFull(forwarder.identifier)
	}
	select {
	case forwarder.tChan <- traces:
		return nil
	case <-forwarder.stopContext.Done():
		return forwarder.stopContext.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DebugDatapoints returns debug level datapoints about this forwarder, including errors processing datapoints
func (forwarder *BufferedForwarder) DebugDatapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.Gauge("datapoint_chan_backup_size", nil, int64(len(forwarder.dpChan))),
		sfxclient.Gauge("event_chan_backup_size", nil, int64(len(forwarder.eChan))),
		sfxclient.Gauge("datapoint_backup_size", nil, atomic.LoadInt64(&forwarder.stats.totalDatapointsBuffered)),
		sfxclient.Gauge("event_backup_size", nil, atomic.LoadInt64(&forwarder.stats.totalEventsBuffered)),
		sfxclient.Gauge("trace_chan_backup_size", nil, int64(len(forwarder.tChan))),
		sfxclient.Gauge("trace_backup_size", nil, atomic.LoadInt64(&forwarder.stats.totalTracesBuffered)),
	}
}

// DefaultDatapoints does nothing and exists to satisfy the protocol.forwarder interface
func (forwarder *BufferedForwarder) DefaultDatapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{}
}

// Datapoints implements the sfxclient.Collector interface and returns all datapoints about the buffered forwarder
func (forwarder *BufferedForwarder) Datapoints() []*datapoint.Datapoint {
	return append(forwarder.DebugDatapoints(), forwarder.DefaultDatapoints()...)
}

// Pipeline for a BufferedForwarder is the total of all buffers and what is in flight
func (forwarder *BufferedForwarder) Pipeline() int64 {
	return int64(len(forwarder.dpChan)) + int64(len(forwarder.eChan)) + atomic.LoadInt64(&forwarder.stats.datapointsInFlight) + atomic.LoadInt64(&forwarder.stats.eventsInFlight) + int64(len(forwarder.tChan)) + atomic.LoadInt64(&forwarder.stats.tracesInFlight)
}

func (forwarder *BufferedForwarder) blockingDrainUpTo() ([]*datapoint.Datapoint, context.Context) {
	// We block the mutex so we allow one drainer to fully drain until we use another
	forwarder.blockingDrainWaitMutex.Lock()
	defer forwarder.blockingDrainWaitMutex.Unlock()

	var currentToken string
	var datapoints []*datapoint.Datapoint
	if len(forwarder.holdDatapoints) > 0 {
		// use hold, this means we hit a batch that wasn't the same token, next thread takes it
		datapoints = forwarder.holdDatapoints
		currentToken = forwarder.getTokenFromDatapoints(datapoints, currentToken)
		forwarder.holdDatapoints = nil
	} else {
		// Block for at least one point
		select {
		case datapoints = <-forwarder.dpChan:
			if forwarder.useAuthFromRequest {
				// all points in a batch will have the same token
				currentToken = forwarder.getTokenFromDatapoints(datapoints, currentToken)
			}
		case <-forwarder.stopContext.Done():
			return []*datapoint.Datapoint{}, forwarder.stopContext
		}
	}

	datapoints, ctx := forwarder.blockingDrainUpToPost(datapoints, currentToken)
	return datapoints, ctx
}

func (forwarder *BufferedForwarder) blockingDrainUpToPost(datapoints []*datapoint.Datapoint, currentToken string) ([]*datapoint.Datapoint, context.Context) {
Loop:
	for int64(len(datapoints)) < *forwarder.config.MaxDrainSize {
		select {
		case datapoint := <-forwarder.dpChan:
			if forwarder.useAuthFromRequest {
				if forwarder.getTokenFromDatapoints(datapoint, "") != currentToken {
					forwarder.holdDatapoints = datapoint
					break Loop
				}
			}
			datapoints = append(datapoints, datapoint...)
			continue
		default:
			// Nothing left.  Flush this.
			break Loop
		}
	}
	atomic.AddInt64(&forwarder.stats.totalDatapointsBuffered, int64(-len(datapoints)))
	ctx := forwarder.stopContext
	if currentToken != "" {
		ctx = context.WithValue(ctx, sfxclient.TokenHeaderName, currentToken)
	}
	return datapoints, ctx
}

func (forwarder *BufferedForwarder) getTokenFromDatapoints(datapoints []*datapoint.Datapoint, currentToken string) string {
	if tok, ok := datapoints[0].Meta[sfxclient.TokenHeaderName]; ok {
		currentToken = tok.(string) // all points in a batch will have the same token
	}
	return currentToken
}

func (forwarder *BufferedForwarder) blockingDrainEventsUpTo() ([]*event.Event, context.Context) {
	// We block the mutex so we allow one drainer to fully drain until we use another
	forwarder.blockingEventDrainWaitMutex.Lock()
	defer forwarder.blockingEventDrainWaitMutex.Unlock()

	var currentToken string
	var events []*event.Event
	if len(forwarder.holdEvents) > 0 {
		// use hold, this means we hit a batch that wasn't the same token, next thread takes it
		events = forwarder.holdEvents
		currentToken = forwarder.getTokenFromEvents(events, currentToken)
		forwarder.holdEvents = nil
	} else {
		// Block for at least one point
		select {
		case events = <-forwarder.eChan:
			if forwarder.useAuthFromRequest {
				// all points in a batch will have the same token
				currentToken = forwarder.getTokenFromEvents(events, currentToken)
			}
		case <-forwarder.stopContext.Done():
			return []*event.Event{}, forwarder.stopContext
		}
	}

	events, ctx := forwarder.blockingDrainEventsUpToPost(events, currentToken)
	return events, ctx
}

func (forwarder *BufferedForwarder) blockingDrainEventsUpToPost(events []*event.Event, currentToken string) ([]*event.Event, context.Context) {
Loop:
	for int64(len(events)) < *forwarder.config.MaxDrainSize {
		select {
		case event := <-forwarder.eChan:
			if forwarder.useAuthFromRequest {
				if forwarder.getTokenFromEvents(event, "") != currentToken {
					forwarder.holdEvents = event
					break Loop
				}
			}
			events = append(events, event...)
			continue
		default:
			// Nothing left.  Flush this.
			break Loop
		}
	}
	atomic.AddInt64(&forwarder.stats.totalEventsBuffered, int64(-len(events)))
	ctx := forwarder.stopContext
	if currentToken != "" {
		ctx = context.WithValue(ctx, sfxclient.TokenHeaderName, currentToken)
	}
	return events, ctx
}

func (forwarder *BufferedForwarder) getTokenFromEvents(events []*event.Event, currentToken string) string {
	if tok, ok := events[0].Meta[sfxclient.TokenHeaderName]; ok {
		currentToken = tok.(string) // all points in a batch will have the same token
	}
	return currentToken
}

func (forwarder *BufferedForwarder) blockingDrainSpansUpTo() ([]*trace.Span, context.Context) {
	// We block the mutex so we allow one drainer to fully drain until we use another
	forwarder.blockingTraceDrainWaitMutex.Lock()
	defer forwarder.blockingTraceDrainWaitMutex.Unlock()

	var currentToken string
	var spans []*trace.Span
	if len(forwarder.holdSpans) > 0 {
		// use hold, this means we hit a batch that wasn't the same token, next thread takes it
		spans = forwarder.holdSpans
		currentToken = forwarder.getTokenFromSpans(spans, currentToken)
		forwarder.holdSpans = nil
	} else {
		// Block for at least one point
		select {
		case spans = <-forwarder.tChan:
			if forwarder.useAuthFromRequest {
				// all points in a batch will have the same token
				currentToken = forwarder.getTokenFromSpans(spans, currentToken)
			}
		case <-forwarder.stopContext.Done():
			return []*trace.Span{}, forwarder.stopContext
		}
	}

	spans, ctx := forwarder.blockingDrainSpansUpToPost(spans, currentToken)
	return spans, ctx
}

func (forwarder *BufferedForwarder) blockingDrainSpansUpToPost(spans []*trace.Span, currentToken string) ([]*trace.Span, context.Context) {
Loop:
	for int64(len(spans)) < *forwarder.config.MaxDrainSize {
		select {
		case span := <-forwarder.tChan:
			if forwarder.useAuthFromRequest {
				if forwarder.getTokenFromSpans(span, "") != currentToken {
					forwarder.holdSpans = span
					break Loop
				}
			}
			spans = append(spans, span...)
			continue
		default:
			// Nothing left.  Flush this.
			break Loop
		}
	}
	atomic.AddInt64(&forwarder.stats.totalTracesBuffered, int64(-len(spans)))
	ctx := forwarder.stopContext
	if currentToken != "" {
		ctx = context.WithValue(ctx, sfxclient.TokenHeaderName, currentToken)
	}
	return spans, ctx
}

func (forwarder *BufferedForwarder) getTokenFromSpans(spans []*trace.Span, currentToken string) string {
	if tok, ok := spans[0].Meta[sfxclient.TokenHeaderName]; ok {
		currentToken = tok.(string) // all points in a batch will have the same token
	}
	return currentToken
}

// Close stops the threads that are flushing channel points to the next forwarder
func (forwarder *BufferedForwarder) Close() error {
	forwarder.stopFunc()
	forwarder.threadsWaitingToDie.Wait()
	return forwarder.closeSender()
}

func (forwarder *BufferedForwarder) doData(drainIndex int) {
	defer forwarder.threadsWaitingToDie.Done()
	logger := log.NewContext(forwarder.logger).With("draining_index", drainIndex)
	for forwarder.stopContext.Err() == nil {
		datapoints, ctx := forwarder.blockingDrainUpTo()
		if len(datapoints) == 0 {
			continue
		}
		logDpIfFlag(logger, forwarder.checker, datapoints, "about to send datapoint")
		atomic.AddInt64(&forwarder.stats.datapointsInFlight, int64(len(datapoints)))
		err := forwarder.sendTo.AddDatapoints(ctx, datapoints)
		atomic.AddInt64(&forwarder.stats.datapointsInFlight, -int64(len(datapoints)))
		if err != nil {
			logger.Log(log.Err, err, "error sending datapoints")
		}
		logDpIfFlag(logger, forwarder.checker, datapoints, "Finished sending datapoint")
	}
}

func (forwarder *BufferedForwarder) doEvent(drainIndex int) {
	defer forwarder.threadsWaitingToDie.Done()
	logger := log.NewContext(forwarder.logger).With("draining_index", drainIndex)
	for forwarder.stopContext.Err() == nil {
		events, ctx := forwarder.blockingDrainEventsUpTo()
		if len(events) == 0 {
			continue
		}
		logEvIfFlag(logger, forwarder.checker, events, "about to send event")
		atomic.AddInt64(&forwarder.stats.eventsInFlight, int64(len(events)))
		err := forwarder.sendTo.AddEvents(ctx, events)
		atomic.AddInt64(&forwarder.stats.eventsInFlight, -int64(len(events)))
		if err != nil {
			logger.Log(log.Err, err, "error sending events")
		}
		logEvIfFlag(logger, forwarder.checker, events, "Finished sending event")
	}
}

func (forwarder *BufferedForwarder) doSpan(drainIndex int) {
	defer forwarder.threadsWaitingToDie.Done()
	logger := log.NewContext(forwarder.logger).With("draining_index", drainIndex)
	for forwarder.stopContext.Err() == nil {
		traces, ctx := forwarder.blockingDrainSpansUpTo()
		if len(traces) == 0 {
			continue
		}
		atomic.AddInt64(&forwarder.stats.tracesInFlight, int64(len(traces)))
		err := forwarder.sendTo.AddSpans(ctx, traces)
		atomic.AddInt64(&forwarder.stats.tracesInFlight, -int64(len(traces)))
		if err != nil {
			logger.Log(log.Err, err, "error sending traces")
		}
	}
}

func (forwarder *BufferedForwarder) start() {
	forwarder.threadsWaitingToDie.Add(int(*forwarder.config.NumDrainingThreads) * 3)
	for i := 0; i < *forwarder.config.NumDrainingThreads; i++ {
		go forwarder.doData(i)
		go forwarder.doEvent(i)
		go forwarder.doSpan(i)
	}
}

type flagChecker interface {
	HasDatapointFlag(*datapoint.Datapoint) bool
	HasEventFlag(*event.Event) bool
}

func logDpIfFlag(l log.Logger, checker flagChecker, dps []*datapoint.Datapoint, msg string) {
	if log.IsDisabled(l) {
		return
	}
	for _, dp := range dps {
		if checker.HasDatapointFlag(dp) {
			l.Log("dp", dp, msg)
		}
	}
}

func logEvIfFlag(l log.Logger, checker flagChecker, ev []*event.Event, msg string) {
	if log.IsDisabled(l) {
		return
	}
	for _, dp := range ev {
		if checker.HasEventFlag(dp) {
			l.Log("ev", dp, msg)
		}
	}
}

// StartupFinished runs the afterStartup method on the forwarder
func (forwarder *BufferedForwarder) StartupFinished() error {
	return forwarder.afterStartup()
}

// NewBufferedForwarder is used only by this package to create a forwarder that buffers its
// datapoint channel
func NewBufferedForwarder(ctx context.Context, config *Config, sendTo Sink, closeIt, afterStartup func() error, logger log.Logger, debugEnpoints func() map[string]http.Handler) *BufferedForwarder {
	config = pointer.FillDefaultFrom(config, DefaultConfig).(*Config)
	logCtx := log.NewContext(logger).With(logkey.Struct, "BufferedForwarder")
	logCtx.Log(logkey.Config, config)
	context, cancel := context.WithCancel(ctx)
	ret := &BufferedForwarder{
		stopFunc:           cancel,
		stopContext:        context,
		dpChan:             make(chan []*datapoint.Datapoint, *config.BufferSize),
		eChan:              make(chan []*event.Event, *config.BufferSize),
		tChan:              make(chan []*trace.Span, *config.BufferSize),
		config:             config,
		sendTo:             sendTo,
		closeSender:        closeIt,
		afterStartup:       afterStartup,
		logger:             logCtx,
		checker:            config.Checker,
		cdim:               config.Cdim,
		identifier:         *config.Name,
		debugEndpoints:     debugEnpoints,
		useAuthFromRequest: *config.UseAuthFromRequest,
	}
	ret.start()
	return ret
}
