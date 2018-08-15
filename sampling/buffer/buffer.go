package buffer

import (
	"context"
	"errors"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/golib/trace"
	"sync"
	"sync/atomic"
	"time"
)

// BuffTrace buffers traces until we send them along or clean them up
// TODO do we want to add a way to limit cache size clean up oldest traces if we run out?
type BuffTrace struct {
	traces   map[string][]*trace.Span // buffer of spans by trace id
	last     map[string]time.Time     // last time we saw a span for this trace id
	remember map[string]time.Time     // cache of traceIDs we keep around when draining ch
	expiry   time.Duration
	interval time.Duration
	sink     trace.Sink
	ch       chan *trace.Span
	sampleCh chan *samplePayload
	done     chan struct{}
	dps      chan chan []*datapoint.Datapoint
	wg       sync.WaitGroup
	logger   log.Logger
	stats    struct {
		dropsBuffFull int64
	}
	tk timekeeper.TimeKeeper
}

func (b *BuffTrace) drain() {
	defer b.wg.Done()
	for {
		select {
		case <-b.done:
			b.drainFinal()
			return
		case s := <-b.ch:
			b.chInner(s)
		case p := <-b.sampleCh:
			b.sampleInner(p)
		case <-b.tk.After(b.interval):
			b.cleanInner()
		case resp := <-b.dps:
			resp <- b.dpsInner()
		}
	}
}

func (b *BuffTrace) drainFinal() {
	for {
		select {
		case s := <-b.ch:
			b.chInner(s)
		case p := <-b.sampleCh:
			b.sampleInner(p)
		default:
			return
		}
	}
}

func (b *BuffTrace) cleanInner() {
	rememberRaz := b.tk.Now().Add(-time.Minute)
	// remove remembered traceIDs after a minute, that's hopefully enough for us to have drained
	for k, v := range b.remember {
		if rememberRaz.After(v) {
			delete(b.remember, k)
		}
	}
	razor := b.tk.Now().Add(-b.expiry)
	// remove buffered spans after the expiry
	for k, v := range b.last {
		if razor.After(v) {
			delete(b.traces, k)
			delete(b.last, k)
		}
	}
}

func (b *BuffTrace) sampleInner(p *samplePayload) {
	var err error
	// this trace has been released for some reason, send them all along
	// remove from our buffer so they can be cleaned up, and remember the decision
	if spans, ok := b.traces[*p.traceID]; ok {
		err = b.sink.AddSpans(context.Background(), spans)
		delete(b.traces, *p.traceID)
		delete(b.last, *p.traceID)
	}
	p.resp <- err
	// there's a possibility that there are spans for this traceID in b.ch
	// we need to remember for a while spans we've released
	b.remember[*p.traceID] = b.tk.Now()
}

func (b *BuffTrace) chInner(s *trace.Span) {
	// spans incoming, check if we're draining released spans else buffer them
	// and update the last time we saw a span for that traceID
	now := b.tk.Now()
	if _, ok := b.remember[s.TraceID]; ok {
		log.IfErr(b.logger, b.sink.AddSpans(context.Background(), []*trace.Span{s}))
	} else {
		b.last[s.TraceID] = now
		b.traces[s.TraceID] = append(b.traces[s.TraceID], s)
	}
}

// TODO use sync.pool?
type samplePayload struct {
	traceID *string
	resp    chan error
}

// Release allows a trace to go down the sink and be garbage collected
func (b *BuffTrace) Release(traceID *string) error {
	resp := &samplePayload{
		traceID: traceID,
		resp:    make(chan error),
	}
	b.sampleCh <- resp
	return <-resp.resp
}

var errBufferFull = errors.New("unable to buffer span, span process buffer is full")

// AddSpans implements trace.Sink, all spans are put into buffers by traceID to be release or garbage collected after some timeout
func (b *BuffTrace) AddSpans(ctx context.Context, spans []*trace.Span) (err error) {
	if len(spans) > 0 {
		for _, v := range spans {
			select {
			case b.ch <- v:
			default:
				atomic.AddInt64(&b.stats.dropsBuffFull, 1)
				err = errBufferFull
			}
		}
	}
	return err
}

// Close implements io.Closer
func (b *BuffTrace) Close() error {
	close(b.done)
	b.wg.Wait()
	return nil
}

// Datapoints implements sfxclient.Collector interface, uses a channel to avoid locks
func (b *BuffTrace) Datapoints() []*datapoint.Datapoint {
	resp := make(chan []*datapoint.Datapoint)
	b.dps <- resp
	return <-resp
}
func (b *BuffTrace) dpsInner() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.Cumulative("proxy.tracing.buffer.spansDroppedBufferFull", nil, atomic.LoadInt64(&b.stats.dropsBuffFull)),
		sfxclient.Gauge("proxy.tracing.buffer.numTraces", nil, int64(len(b.traces))),
	}
}

// New gives you a new trace buffer
func New(expiry time.Duration, sink trace.Sink, logger log.Logger) *BuffTrace {
	return newBuff(timekeeper.RealTime{}, expiry, time.Minute, sink, logger, 100000, 100000)
}

func newBuff(tk timekeeper.TimeKeeper, expiry, rotation time.Duration, sink trace.Sink, logger log.Logger, sampleChanSize, spanChanSize int) *BuffTrace {
	ret := &BuffTrace{
		traces:   make(map[string][]*trace.Span),
		last:     make(map[string]time.Time),
		remember: make(map[string]time.Time),
		expiry:   expiry,   // how long till a trace that hasn't been released is garbage collected
		interval: rotation, // how often we iterate through and garbage collect stuff
		sink:     sink,
		ch:       make(chan *trace.Span, spanChanSize),
		sampleCh: make(chan *samplePayload, sampleChanSize),
		done:     make(chan struct{}),
		dps:      make(chan chan []*datapoint.Datapoint, 10),
		logger:   logger,
		tk:       tk,
	}
	ret.wg.Add(1)
	go ret.drain()
	return ret
}
