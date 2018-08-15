package histo

import (
	"fmt"
	"github.com/rcrowley/go-metrics"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/timekeeper"
	"sync"
	"sync/atomic"
	"time"
)

// SpanIdentity is a tuple of Service and Operation
type SpanIdentity struct {
	Service   string
	Operation string
}

func (k *SpanIdentity) dims() map[string]string {
	return map[string]string{
		"service":   k.Service,
		"operation": k.Operation,
	}
}

// SpanHistoBag emits metrics for spans/identities
type SpanHistoBag struct {
	digests       map[SpanIdentity]metrics.Histogram
	last          map[SpanIdentity]time.Time
	expiry        time.Duration
	interval      time.Duration
	reservoirSize int
	alphaFactor   float64
	ch            chan *payload
	done          chan struct{}
	dpsCh         chan chan []*datapoint.Datapoint
	quantiles     []float64
	stats         struct {
		dropsBufferFull int64
		totalSamples    int64
	}
	wg sync.WaitGroup
	tk timekeeper.TimeKeeper
}

func (b *SpanHistoBag) drainFinal() {
	for {
		select {
		case s := <-b.ch:
			b.payloadInner(s)
		default:
			return
		}
	}
}

func (b *SpanHistoBag) drain() {
	defer b.wg.Done()
	for {
		select {
		case <-b.done:
			// drain internal channel before exiting
			b.drainFinal()
			return
		case <-b.tk.After(b.interval):
			b.cleanInner()
		case s := <-b.ch:
			b.payloadInner(s)
		case resp := <-b.dpsCh:
			resp <- b.dpsInner()
		}
	}
}

func (b *SpanHistoBag) dpsInner() []*datapoint.Datapoint {
	dps := make([]*datapoint.Datapoint, 0, len(b.digests)*(3+len(b.quantiles))+2)
	for k, v := range b.digests {
		dims := k.dims()
		// TODO these are temporary names
		// TODO where does the sf_metricized get added?
		dps = append(dps, []*datapoint.Datapoint{
			sfxclient.Cumulative("proxy.tracing.count", dims, v.Count()),
			sfxclient.Gauge("proxy.tracing.min", dims, v.Min()),
			sfxclient.Gauge("proxy.tracing.max", dims, v.Max()),
		}...)
		qv := v.Percentiles(b.quantiles)
		for i, q := range b.quantiles {
			dps = append(dps, sfxclient.GaugeF(fmt.Sprintf("proxy.tracing.%f", q), dims, qv[i]))
		}
	}
	dps = append(dps, sfxclient.Cumulative("proxy.tracing.droppedSamples", map[string]string{"reason": "buffer_full"}, atomic.LoadInt64(&b.stats.dropsBufferFull)))
	dps = append(dps, sfxclient.Cumulative("proxy.tracing.totalSamples", nil, b.stats.totalSamples))
	dps = append(dps, sfxclient.Gauge("proxy.tracing.totalUnique", nil, int64(len(b.digests))))
	return dps
}

func (b *SpanHistoBag) cleanInner() {
	// clean out any traces that haven't reported any metrics in a while
	razor := b.tk.Now().Add(-b.expiry)
	for k, v := range b.last {
		if razor.After(v) {
			delete(b.digests, k)
			delete(b.last, k)
		}
	}

}

// update last time we saw a span for this traceId, create a histogram if necessary and update it
func (b *SpanHistoBag) payloadInner(s *payload) {
	b.last[*s.identity] = b.tk.Now()
	h, ok := b.digests[*s.identity]
	if !ok {
		// TODO this isn't the optimal histogram for this...
		// TODO at least with this histogram we could use a sync.pool because they can be cleared... eh?
		h = metrics.NewHistogram(metrics.NewExpDecaySample(b.reservoirSize, b.alphaFactor))
		b.digests[*s.identity] = h
	}
	h.Update(int64(s.sample))
	b.stats.totalSamples++
}

// TODO sync.pool me?
type payload struct {
	identity *SpanIdentity
	sample   float64
}

// Update adds a sample to channel, don't block
func (b *SpanHistoBag) Update(identity *SpanIdentity, sample float64) {
	select {
	case b.ch <- &payload{
		identity: identity,
		sample:   sample,
	}:
	default:
		atomic.AddInt64(&b.stats.dropsBufferFull, 1)
	}
}

// Close the object
func (b *SpanHistoBag) Close() error {
	close(b.done)
	b.wg.Wait()
	return nil
}

// Datapoints returns datapoins for all histograms, but lockless requires a request
func (b *SpanHistoBag) Datapoints() []*datapoint.Datapoint {
	resp := make(chan []*datapoint.Datapoint)
	b.dpsCh <- resp
	return <-resp
}

const (
	// DefaultMetricsReservoirSize is a default for histogram creation
	DefaultMetricsReservoirSize = 1028
	// DefaultMetricsAlphaFactor is a default for histogram creation
	DefaultMetricsAlphaFactor = 0.015
)

// DefaultQuantiles is the default value for the quantiles
var DefaultQuantiles = []float64{0.50, 0.90, 0.99}

// New gets you one
func New(spanExpiry time.Duration, quantiles []float64) *SpanHistoBag {
	return newBag(timekeeper.RealTime{}, spanExpiry, time.Minute, 100000, DefaultMetricsReservoirSize, DefaultMetricsAlphaFactor, quantiles)
}
func newBag(tk timekeeper.TimeKeeper, digestExpiry, cleanInterval time.Duration, chanSize, metricsReservoirSize int, metricsAlphaFactor float64, quantiles []float64) *SpanHistoBag {
	bag := &SpanHistoBag{
		digests:       make(map[SpanIdentity]metrics.Histogram),
		last:          make(map[SpanIdentity]time.Time),
		ch:            make(chan *payload, chanSize),
		done:          make(chan struct{}),
		dpsCh:         make(chan chan []*datapoint.Datapoint, 10),
		expiry:        digestExpiry,
		interval:      cleanInterval,
		reservoirSize: metricsReservoirSize,
		alphaFactor:   metricsAlphaFactor,
		quantiles:     quantiles,
		tk:            tk,
	}
	bag.wg.Add(1)
	go bag.drain()
	return bag
}
