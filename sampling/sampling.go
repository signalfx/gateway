package sampling

import (
	"context"
	"fmt"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/golib/trace"
	"github.com/signalfx/metricproxy/sampling/bloom"
	"github.com/signalfx/metricproxy/sampling/leakybucket"
	"github.com/signalfx/sfxinternalgo/lib/logkey"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// SampleObj is the json config object
type SampleObj struct {
	BaseRate          *float64 `json:",omitempty"`
	MaxSPM            *int64   `json:",omitempty"`
	FalsePositiveRate *float64 `json:",omitempty"`
	Capacity          *int64   `json:",omitempty"`
	CyclePeriod       *string  `json:",omitempty"` // this is half how long we'll remember a trace
	Adapt             *bool    `json:",omitempty"`
	BackupLocation    *string  `json:",omitempty"`
}

var defaultSampleObj = &SampleObj{
	BaseRate:          pointer.Float64(0.01),
	MaxSPM:            pointer.Int64(10000),
	Capacity:          pointer.Int64(1000000),
	CyclePeriod:       pointer.String("5m"),
	FalsePositiveRate: pointer.Float64(0.0001),
	Adapt:             pointer.Bool(false),
	BackupLocation:    pointer.String("/tmp"),
}

// not thread safe when it comes to rotations vs has and add
type bloomSet struct {
	elder             *bloomfilter.Bloom
	younger           *bloomfilter.Bloom
	capacity          uint64
	newCap            int64
	falsePositiveRate float64
	adapt             bool
	logger            log.Logger
	location          string
}

func (set *bloomSet) rotateBlooms() {
	capacity := set.capacity
	newCap := uint64(atomic.LoadInt64(&set.newCap) * 2) // the life of any bloom filter is twice the cycle period
	if set.adapt && newCap > capacity {
		capacity = newCap
		set.logger.Log(logkey.Size, capacity, "adapting to new size")
		atomic.StoreInt64(&set.newCap, 0)
	}

	set.elder = set.younger
	set.younger = bloomfilter.New(capacity, set.falsePositiveRate)
}

func (set *bloomSet) has(low, high uint64) bool {
	atomic.AddInt64(&set.newCap, 1)
	if set.elder.Has(low, high) {
		set.younger.Add(low, high) // only elder might have it, want to persist things we know to be in the set
		return true
	}
	return false
}

func (set *bloomSet) add(low, high uint64) {
	set.younger.Add(low, high)
	set.elder.Add(low, high)
}

func (set *bloomSet) write() {
	set.writeBf(set.younger, "younger")
	set.writeBf(set.elder, "elder")
}

func (set *bloomSet) writeBf(bf *bloomfilter.Bloom, file string) {
	var f *os.File
	var err error
	if f, err = os.Create(set.location + "_" + file); err == nil {
		buff := make([]byte, 65536)
		err = bf.Write(f, buff)
	}
	log.IfErr(set.logger, errors.Annotate(err, "Could not write out bloomfilter"))
}

func newBloomSet(location string, cap uint64, falsePos float64, adapt bool, logger log.Logger) *bloomSet {
	set := &bloomSet{
		falsePositiveRate: falsePos,
		capacity:          cap,
		adapt:             adapt,
		logger:            logger,
		location:          location,
	}
	set.elder = set.getBloom("elder")
	set.younger = set.getBloom("younger")
	return set
}

func (set *bloomSet) getBloom(file string) *bloomfilter.Bloom {
	var b *bloomfilter.Bloom
	var err error
	if b, err = bloomfilter.NewFromFile(set.location + "_" + file); err != nil {
		b = bloomfilter.New(set.capacity, set.falsePositiveRate)
	}
	log.IfErr(set.logger, errors.Annotate(err, fmt.Sprintf("unable to load bloom from %s, %s", set.location, file)))
	return b
}

// SampleForwarder is a trace sampler
type SampleForwarder struct {
	baseRate          float64
	capacity          uint64
	falsePositiveRate float64
	memory            time.Duration
	done              chan struct{}
	rotate            chan chan struct{}
	sampledSet        *bloomSet
	notSampledSet     *bloomSet
	randit            *rand.Rand
	wg                sync.WaitGroup
	stats             struct {
		sampled       int64
		notSampled    int64
		droppedClosed int64
		droppedSpm    int64
	}
	throttle *leakybucket.LeakyBucket
}

// AddSpans samples based on TraceId and forwards those onto the next sink
func (f *SampleForwarder) AddSpans(ctx context.Context, spans []*trace.Span, next trace.Sink) (err error) {
	if len(spans) > 0 {
		if f == nil {
			return next.AddSpans(ctx, spans)
		}
		sample := f.sampleTraces(ctx, spans)
		if len(sample) > 0 {
			if err = f.throttle.CheckAction(int64(len(spans))); err == nil {
				f.throttle.Deduct(int64(len(spans)))
				err = next.AddSpans(ctx, sample)
			} else {
				atomic.AddInt64(&f.stats.droppedSpm, int64(len(spans)))
			}
		}
	}
	return err
}

// Datapoints returns the stats
func (f *SampleForwarder) Datapoints() (dps []*datapoint.Datapoint) {
	if f != nil {
		dps = []*datapoint.Datapoint{
			sfxclient.Cumulative("trace.sampled", nil, atomic.LoadInt64(&f.stats.sampled)),
			sfxclient.Cumulative("trace.notSampled", nil, atomic.LoadInt64(&f.stats.notSampled)),
			sfxclient.Cumulative("span.sampler.dropped_maxspm", nil, atomic.LoadInt64(&f.stats.droppedSpm)),
		}
	}
	return dps
}

// get the unique traceIds from the list of spans, then iterate through them deciding whether or not to sample them
// if we don't already know about them
// also check if we need to rotate first
func (f *SampleForwarder) sampleTraces(ctx context.Context, spans []*trace.Span) (allowed []*trace.Span) {
	select {
	case c := <-f.rotate:
		f.sampledSet.rotateBlooms()
		f.notSampledSet.rotateBlooms()
		c <- struct{}{}
	default:
	}
	ids := make(map[string][]*trace.Span, len(spans)) // start from zero better perf? test it.
	for _, s := range spans {
		i, ok := ids[s.TraceID]
		if !ok {
			i = make([]*trace.Span, 0, 1)
		}
		i = append(i, s)
		ids[s.TraceID] = i
	}
	for i, ss := range ids {
		if f.sample(i) {
			allowed = append(allowed, ss...)
		}
	}

	return allowed
}

func (f *SampleForwarder) sample(id string) (ret bool) {
	size := len(id)
	var high, low uint64
	var err1, err2 error
	switch size {
	case 32:
		high, err1 = strconv.ParseUint(id[16:], 16, 64)
		fallthrough
	case 16:
		low, err2 = strconv.ParseUint(id[:16], 16, 64)
	default:
		return false // not a real id
	}
	if err1 != nil || err2 != nil {
		return false
	}
	if f.sampledSet.has(low, high) {
		return true
	}
	if f.notSampledSet.has(low, high) {
		return false
	}
	if f.randit.Float64() < f.baseRate {
		f.sampledSet.add(low, high)
		atomic.AddInt64(&f.stats.sampled, 1)
		return true
	}
	f.notSampledSet.add(low, high)
	atomic.AddInt64(&f.stats.notSampled, 1)
	return false
}

func (f *SampleForwarder) tickTock() {
	for {
		select {
		case <-f.done:
			close(f.rotate)
			f.wg.Done()
			return
		case <-time.After(f.memory):
			c := make(chan struct{})
			f.rotate <- c
			<-c
		}
	}
}

// Close closes the obj
func (f *SampleForwarder) Close() {
	if f != nil {
		close(f.done)
		f.wg.Wait()
		f.sampledSet.write()
		f.notSampledSet.write()
	}
}

// New gets you a new Sampler
func New(conf *SampleObj, logger log.Logger) (ret *SampleForwarder, err error) {
	if conf != nil {
		conf = pointer.FillDefaultFrom(conf, defaultSampleObj).(*SampleObj)
		memory, err := time.ParseDuration(*conf.CyclePeriod)
		if err != nil {
			return nil, err
		}
		ret = &SampleForwarder{
			done:              make(chan struct{}),
			rotate:            make(chan chan struct{}, 10),
			baseRate:          *conf.BaseRate,
			capacity:          uint64(*conf.Capacity),
			falsePositiveRate: *conf.FalsePositiveRate,
			memory:            memory,
		}
		ret.throttle = leakybucket.New(timekeeper.RealTime{}, *conf.MaxSPM, time.Minute)

		ret.sampledSet = newBloomSet(path.Join(*conf.BackupLocation, "sampledBloomSet"), ret.capacity, ret.falsePositiveRate, *conf.Adapt, logger)
		ret.notSampledSet = newBloomSet(path.Join(*conf.BackupLocation, "notSampledBloomSet"), ret.capacity, ret.falsePositiveRate, *conf.Adapt, logger)
		ret.randit = rand.New(rand.NewSource(time.Now().UnixNano()))
		ret.wg.Add(1)
		go ret.tickTock()
	}
	return ret, nil
}
