package buffer

import (
	"compress/gzip"
	"context"
	"github.com/mailru/easyjson"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/golib/trace"
	"github.com/signalfx/metricproxy/logkey"
	"github.com/signalfx/metricproxy/sampling/buffer/encoding"
	"github.com/signalfx/sfxinternalgo/cmd/sbingest/common"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// BuffTrace buffers Traces until we send them along or clean them up
// TODO do we want to add a way to limit cache size clean up oldest Traces if we run out?
type BuffTrace struct {
	encoding.OnDisk
	expiry          time.Duration
	interval        time.Duration
	sink            trace.Sink
	ch              chan *trace.Span
	releaseCh       chan *samplePayload
	done            chan struct{}
	cleanCh         chan struct{}
	dps             chan chan []*datapoint.Datapoint
	storageLocation string
	wg              sync.WaitGroup
	logger          log.Logger
	stats           struct {
		dropsBuffFull     int64
		numSpansReleased  int64
		numTracesReleased int64
		numSpansAgedOut   int64
		numTracesAgedOut  int64
	}
	tk timekeeper.TimeKeeper
}

func (b *BuffTrace) cleanCycle() {
	defer b.wg.Done()
	for {
		select {
		case <-b.done:
			return
		case <-b.tk.After(b.interval):
			b.cleanCh <- struct{}{}
		}
	}

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
		case p := <-b.releaseCh:
			b.releaseInner(p)
		case <-b.cleanCh:
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
		case p := <-b.releaseCh:
			b.releaseInner(p)
		default:
			return
		}
	}
}

func (b *BuffTrace) cleanInner() {
	rememberRaz := b.tk.Now().Add(-time.Minute)
	// remove remembered traceIDs after a minute, that's hopefully enough for us to have drained
	for k, v := range b.Remember {
		if rememberRaz.After(v) {
			delete(b.Remember, k)
		}
	}
	razor := b.tk.Now().Add(-b.expiry)
	// remove buffered spans after the expiry
	for k, v := range b.Last {
		if razor.After(v) {
			b.cleanBufferOfTrace(&k, true)
		}
	}
}

func (b *BuffTrace) releaseInner(p *samplePayload) {
	var err error
	// this trace has been released for some reason, send them all along
	// remove from our buffer so they can be cleaned up, and Remember the decision
	if spans, ok := b.Traces[*p.traceID]; ok {
		err = b.sink.AddSpans(context.Background(), spans)
		b.cleanBufferOfTrace(p.traceID, false)
	}
	p.resp <- err
	// there's a possibility that there are spans for this traceID in b.ch
	// we need to Remember for a while spans we've released
	b.Remember[*p.traceID] = b.tk.Now()
}

func (b *BuffTrace) cleanBufferOfTrace(id *string, clean bool) {
	num := int64(len(b.Traces[*id]))
	b.NumSpans -= num
	if clean {
		b.stats.numSpansAgedOut += num
		b.stats.numTracesAgedOut++
	} else {
		b.stats.numSpansReleased += num
		b.stats.numTracesReleased++
	}
	delete(b.Traces, *id)
	delete(b.Last, *id)
}

func (b *BuffTrace) chInner(s *trace.Span) {
	// spans incoming, check if we're draining released spans else buffer them
	// and update the Last time we saw a span for that traceID
	now := b.tk.Now()
	if _, ok := b.Remember[s.TraceID]; ok {
		log.IfErr(b.logger, b.sink.AddSpans(context.Background(), []*trace.Span{s}))
	} else {
		b.Last[s.TraceID] = now
		b.Traces[s.TraceID] = append(b.Traces[s.TraceID], s)
		b.NumSpans++
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
	b.releaseCh <- resp
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
	return b.writeOut()
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
		sfxclient.Gauge("proxy.tracing.buffer.totalBufferedTraces", nil, int64(len(b.Traces))),
		sfxclient.Gauge("proxy.tracing.buffer.totalBufferedSpans", nil, b.NumSpans),
		sfxclient.Cumulative("proxy.tracing.buffer.numTracesAgedOut", nil, b.stats.numTracesAgedOut),
		sfxclient.Cumulative("proxy.tracing.buffer.numSpansAgedOut", nil, b.stats.numSpansAgedOut),
		sfxclient.Cumulative("proxy.tracing.buffer.numTracesReleased", nil, b.stats.numTracesReleased),
		sfxclient.Cumulative("proxy.tracing.buffer.numSpansReleased", nil, b.stats.numSpansReleased),
	}
}

func (b *BuffTrace) writeOut() error {
	var err error
	var f *os.File
	if f, err = os.Create(b.storageLocation); err == nil {
		zw := gzip.NewWriter(f)
		_, err = easyjson.MarshalToWriter(b, zw)
		b.logger.Log(logkey.ContentLength, b.NumSpans, "num spans out")
		log.IfErr(b.logger, common.FirstNonNil(zw.Close(), f.Close()))
	}
	return err
}

// New gives you a new trace buffer
func New(storageLocation string, expiry time.Duration, sink trace.Sink, logger log.Logger) *BuffTrace {
	return newBuff(storageLocation, timekeeper.RealTime{}, expiry, time.Minute, sink, logger, 100000, 100000)
}

func getFromFile(storage string, logger log.Logger) (ret *encoding.OnDisk) {
	var f *os.File
	var err error
	if f, err = os.Open(storage); err == nil {
		var rr io.ReadCloser
		rr, err = gzip.NewReader(f)
		if err == nil {
			ret = &encoding.OnDisk{}
			err = easyjson.UnmarshalFromReader(rr, ret)
			if err == nil {
				logger.Log(logkey.Filename, storage, "Successfully read buffer from file")
				logger.Log(logkey.ContentLength, ret.NumSpans, "num spans in")
			}
			log.IfErr(logger, rr.Close())
		}
		log.IfErr(logger, f.Close())
	}
	return ret
}

func newBuff(storage string, tk timekeeper.TimeKeeper, expiry, rotation time.Duration, sink trace.Sink, logger log.Logger, sampleChanSize, spanChanSize int) *BuffTrace {
	od := getFromFile(storage, logger)
	if od == nil {
		od = &encoding.OnDisk{}
	}

	if od.Remember == nil {
		od.Remember = make(map[string]time.Time)
	}
	if od.Last == nil {
		od.Last = make(map[string]time.Time)
	}
	if od.Traces == nil {
		od.Traces = make(map[string][]*trace.Span)
	}

	ret := &BuffTrace{
		OnDisk:          *od,
		expiry:          expiry,   // how long till a trace that hasn't been released is garbage collected
		interval:        rotation, // how often we iterate through and garbage collect stuff
		sink:            sink,
		ch:              make(chan *trace.Span, spanChanSize),
		releaseCh:       make(chan *samplePayload, sampleChanSize),
		done:            make(chan struct{}),
		cleanCh:         make(chan struct{}, 2),
		dps:             make(chan chan []*datapoint.Datapoint, 2),
		logger:          logger,
		tk:              tk,
		storageLocation: storage,
	}

	ret.wg.Add(2)
	go ret.cleanCycle()
	go ret.drain()
	return ret
}
