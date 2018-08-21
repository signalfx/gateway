package histo

import (
	"compress/gzip"
	"fmt"
	"github.com/mailru/easyjson"
	"github.com/signalfx/go-metrics"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/metricproxy/sampling/histo/encoding"
	"github.com/signalfx/sfxinternalgo/cmd/sbingest/common"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// SpanHistoBag emits metrics for spans/identities
type SpanHistoBag struct {
	digests       map[encoding.SpanIdentity]metrics.Histogram
	last          map[encoding.SpanIdentity]time.Time
	expiry        time.Duration
	interval      time.Duration
	reservoirSize int
	alphaFactor   float64
	ch            chan *payload
	done          chan struct{}
	cleanCh       chan struct{}
	dpsCh         chan chan []*datapoint.Datapoint
	location      string
	logger        log.Logger
	quantiles     []float64
	stats         struct {
		dropsBufferFull int64
		totalSamples    int64
		numCleaned      int64
	}
	wg sync.WaitGroup
	tk timekeeper.TimeKeeper
}

func (b *SpanHistoBag) cleanCycle() {
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

func (b *SpanHistoBag) drainFinal() {
	defer b.logger.Log("finishing drainFinal")
	for {
		select {
		case s := <-b.ch:
			b.payloadInner(s)
		default:
			return
		}
	}
}

// this is lockless, so all things that modify or read from those things being modified are done via channels read from here
func (b *SpanHistoBag) drain() {
	defer b.wg.Done()
	for {
		select {
		case <-b.done:
			// drain internal channel before exiting
			b.drainFinal()
			return
		case <-b.cleanCh:
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
		dims := k.Dims()
		// TODO these are temporary names
		// TODO where does the sf_metricized get added?
		dps = append(dps, []*datapoint.Datapoint{
			sfxclient.Cumulative("proxy.tracing.count", dims, v.Count()),
			sfxclient.Gauge("proxy.tracing.min", dims, v.Min()),
			sfxclient.Gauge("proxy.tracing.max", dims, v.Max()),
		}...)
		qv := v.Percentiles(b.quantiles)
		for i, q := range b.quantiles {
			dps = append(dps, sfxclient.GaugeF(fmt.Sprintf("proxy.tracing.p%d", int(q*100)), dims, qv[i]))
		}
	}
	dps = append(dps, sfxclient.Cumulative("proxy.tracing.droppedSamples", map[string]string{"reason": "buffer_full"}, atomic.LoadInt64(&b.stats.dropsBufferFull)))
	dps = append(dps, sfxclient.Cumulative("proxy.tracing.totalSamples", nil, b.stats.totalSamples))
	dps = append(dps, sfxclient.Gauge("proxy.tracing.totalUnique", nil, int64(len(b.digests))))
	dps = append(dps, sfxclient.Cumulative("proxy.tracing.numCleaned", nil, b.stats.numCleaned))
	return dps
}

func (b *SpanHistoBag) cleanInner() {
	// clean out any traces that haven't reported any metrics in a while
	razor := b.tk.Now().Add(-b.expiry)
	for k, v := range b.last {
		if razor.After(v) {
			b.stats.numCleaned++
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
	identity *encoding.SpanIdentity
	sample   float64
}

// Update adds a sample to channel, don't block
func (b *SpanHistoBag) Update(identity *encoding.SpanIdentity, sample float64) {
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
	return b.writeOut()
}

func (b *SpanHistoBag) getFromFile(storage string) (ret *encoding.OnDisk, err error) {
	var f *os.File
	if f, err = os.Open(storage); err == nil {
		var rr io.ReadCloser
		rr, err = gzip.NewReader(f)
		ret = &encoding.OnDisk{}
		if err == nil {
			err = easyjson.UnmarshalFromReader(rr, ret)
			log.IfErr(b.logger, rr.Close())
		}
		log.IfErr(b.logger, f.Close())
	}
	return ret, err
}

func (b *SpanHistoBag) writeOut() error {
	var err error
	var f io.WriteCloser
	if f, err = os.Create(b.location); err == nil {
		zw := gzip.NewWriter(f)
		og := encoding.OnDisk{
			Digests:              make(map[encoding.SpanIdentity][]int64, len(b.digests)),
			Last:                 b.last,
			MetricsAlphaFactor:   b.alphaFactor,
			MetricsReservoirSize: b.reservoirSize,
		}
		for k, v := range b.digests {
			og.Digests[k] = v.Values()
		}
		_, err = easyjson.MarshalToWriter(&og, zw)
		log.IfErr(b.logger, common.FirstNonNil(zw.Close(), f.Close()))
	}
	return err
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
func New(storageLocation string, logger log.Logger, spanExpiry time.Duration, quantiles []float64) *SpanHistoBag {
	return newBag(storageLocation, logger, timekeeper.RealTime{}, spanExpiry, time.Minute, 100000, DefaultMetricsReservoirSize, DefaultMetricsAlphaFactor, quantiles)
}

func newBag(location string, logger log.Logger, tk timekeeper.TimeKeeper, digestExpiry, cleanInterval time.Duration, chanSize, metricsReservoirSize int, metricsAlphaFactor float64, quantiles []float64) *SpanHistoBag {
	bag := &SpanHistoBag{
		ch:            make(chan *payload, chanSize),
		done:          make(chan struct{}),
		cleanCh:       make(chan struct{}, 2),
		dpsCh:         make(chan chan []*datapoint.Datapoint, 2),
		expiry:        digestExpiry,
		interval:      cleanInterval,
		reservoirSize: metricsReservoirSize,
		alphaFactor:   metricsAlphaFactor,
		quantiles:     quantiles,
		tk:            tk,
		location:      location,
		logger:        logger,
	}
	bag.updateFromDisk(location)

	bag.wg.Add(2)
	go bag.cleanCycle()
	go bag.drain()
	return bag
}

func (b *SpanHistoBag) updateFromDisk(location string) {
	od, err := b.getFromFile(location)
	log.IfErr(b.logger, errors.Annotate(err, "Error reading from disk, using new structure"))
	if od != nil && len(od.Digests) > 0 {
		b.digests = make(map[encoding.SpanIdentity]metrics.Histogram, len(od.Digests))
		b.last = od.Last
		for k, v := range od.Digests {
			b.digests[k] = metrics.NewHistogram(metrics.NewExpDecaySampleWithValues(od.MetricsReservoirSize, od.MetricsAlphaFactor, v))
		}
	} else {
		b.digests = make(map[encoding.SpanIdentity]metrics.Histogram)
		b.last = make(map[encoding.SpanIdentity]time.Time)
	}
}
