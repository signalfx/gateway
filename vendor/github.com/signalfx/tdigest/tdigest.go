package tdigest

import (
	"fmt"
	"math"
	"sort"
)

type TDigest struct {
	Scaler      scaler
	Compression float64

	maxProcessed      int
	maxUnprocessed    int
	processed         CentroidList
	unprocessed       CentroidList
	cumulative        []float64
	processedWeight   float64
	unprocessedWeight float64
	min               float64
	max               float64
	count             int64
	decayCount        int32
	decayEvery        int32
	decayValue        float64
}

func New() *TDigest {
	return NewWithCompression(1000)
}

func NewWithCompression(c float64) *TDigest {
	return NewWithDecay(c, 0, 0)
}

var defaultScaler = &K3Spliced{}

func NewWithDecay(compression, decayValue float64, decayEvery int32) *TDigest {
	return NewWithScaler(defaultScaler, compression, decayValue, decayEvery)
}

func NewWithScaler(scale scaler, compression, decayValue float64, decayEvery int32) *TDigest {
	t := &TDigest{
		Compression: compression,
		Scaler:      scale,
		decayValue:  decayValue,
		decayEvery:  decayEvery,
	}
	t.maxProcessed = t.Scaler.processedSize(t.Compression)
	t.maxUnprocessed = t.Scaler.unprocessedSize(t.Compression)
	t.processed = make([]Centroid, 0, t.maxProcessed)
	t.unprocessed = make([]Centroid, 0, t.maxUnprocessed+1)
	t.cumulative = make([]float64, 0, t.maxProcessed+1)
	t.min = math.MaxFloat64
	t.max = -math.MaxFloat64
	return t
}

func (t *TDigest) Clear() {
	t.processed.Clear()
	t.unprocessed.Clear()
	t.cumulative = t.cumulative[:0]
	t.min = math.MaxFloat64
	t.max = -math.MaxFloat64
	t.count = 0
	t.decayCount = 0
	t.processedWeight = 0
	t.unprocessedWeight = 0
}

func (t *TDigest) Add(x, w float64) {
	if math.IsNaN(x) {
		return
	}
	t.AddCentroid(Centroid{Mean: x, Weight: w})
}

func (t *TDigest) handleDecay(w float64) {
	t.count++
	if t.decayValue > 0 {
		t.decayCount += int32(w)
		if t.decayCount >= t.decayEvery {
			t.decay()
			t.decayCount = 0
		}
	}
}

func (t *TDigest) Centroids() CentroidList {
	t.process()
	return t.processed
}

func (t *TDigest) AddCentroidList(c CentroidList) {
	l := c.Len()
	for i := 0; i < l; i++ {
		diff := l - i
		room := t.maxUnprocessed - t.unprocessed.Len()
		mid := i + diff
		if room < diff {
			mid = i + room
		}
		for i < mid {
			t.AddCentroid(c[i])
			i++
		}
	}
}

func (t *TDigest) AddCentroid(c Centroid) {
	t.unprocessed = append(t.unprocessed, c)
	t.unprocessedWeight += c.Weight

	if t.processed.Len() >= t.maxProcessed || t.unprocessed.Len() > (t.maxUnprocessed-t.processed.Len()) {
		t.process()
	}
	t.handleDecay(c.Weight)
}

func (t *TDigest) process() {
	t.processIt(true)
}

func (t *TDigest) processIt(updateCumulative bool) {
	if t.unprocessed.Len() > 0 || t.processed.Len() > t.maxProcessed {
		// Append all processed centroids to the unprocessed list and sort
		t.unprocessed = append(t.unprocessed, t.processed...)
		sort.Sort(&t.unprocessed)

		// Reset processed list with first centroid
		t.processed.Clear()
		t.processed = append(t.processed, t.unprocessed[0])

		t.processedWeight += t.unprocessedWeight
		t.unprocessedWeight = 0
		normalizer := t.Scaler.normalizer(t.Compression, t.processedWeight)
		soFar := t.unprocessed[0].Weight
		k1 := t.Scaler.k(0, normalizer)
		limit := t.processedWeight * t.Scaler.q(k1+1, normalizer)
		for _, centroid := range t.unprocessed[1:] {
			projected := soFar + centroid.Weight
			if projected <= limit {
				soFar = projected
				_ = (&t.processed[t.processed.Len()-1]).Add(centroid) // igonoring error, would have erred when we first added it not here
			} else {
				k1 = t.Scaler.k(soFar/t.processedWeight, normalizer)
				limit = t.processedWeight * t.Scaler.q(k1+1.0, normalizer)
				soFar += centroid.Weight
				t.processed = append(t.processed, centroid)
			}
			if centroid.Mean < t.min {
				t.min = centroid.Mean
			}
			if centroid.Mean > t.max {
				t.max = centroid.Mean
			}
		}
		if updateCumulative {
			t.updateCumulative()
		}
		t.unprocessed.Clear()
	}
}

func (t *TDigest) updateCumulative() {
	t.cumulative = t.cumulative[:0]
	prev := 0.0
	for _, centroid := range t.processed {
		cur := centroid.Weight
		t.cumulative = append(t.cumulative, prev+cur/2.0)
		prev += cur
	}
	t.cumulative = append(t.cumulative, prev)
}

func (t *TDigest) Quantile(q float64) float64 {
	t.process()
	if q < 0 || q > 1 || t.processed.Len() == 0 {
		return math.NaN()
	}
	if t.processed.Len() == 1 {
		return t.processed[0].Mean
	}
	index := q * t.processedWeight
	if index <= t.processed[0].Weight/2.0 {
		return t.min + 2.0*index/t.processed[0].Weight*(t.processed[0].Mean-t.min)
	}

	lower := sort.Search(len(t.cumulative), func(i int) bool {
		return t.cumulative[i] >= index
	})

	if lower+1 != len(t.cumulative) {
		z1 := index - t.cumulative[lower-1]
		z2 := t.cumulative[lower] - index
		return weightedAverage(t.processed[lower-1].Mean, z2, t.processed[lower].Mean, z1)
	}

	z1 := index - t.processedWeight - t.processed[lower-1].Weight/2.0
	z2 := (t.processed[lower-1].Weight / 2.0) - z1
	return weightedAverage(t.processed[t.processed.Len()-1].Mean, z1, t.max, z2)
}

func (t *TDigest) CDF(x float64) float64 {
	t.process()
	switch t.processed.Len() {
	case 0:
		return 0.0
	case 1:
		if x <= t.min {
			return 0.0
		}
		if x >= t.max {
			return 1.0
		}
		width := t.max - t.min
		if (x - t.min) <= width {
			// min and max are too close together to do any viable interpolation
			return 0.5
		}
		return (x - t.min) / width
	}

	if x <= t.min {
		return 0.0
	}
	if x >= t.max {
		return 1.0
	}
	m0 := t.processed[0].Mean
	// Left Tail
	if x <= m0 {
		if m0-t.min > 0 {
			return (x - t.min) / (m0 - t.min) * t.processed[0].Weight / t.processedWeight / 2.0
		}
		return 0.0
	}
	// Right Tail
	mn := t.processed[t.processed.Len()-1].Mean
	if x >= mn {
		if t.max-mn > 0.0 {
			return 1.0 - (t.max-x)/(t.max-mn)*t.processed[t.processed.Len()-1].Weight/t.processedWeight/2.0
		}
		return 1.0
	}

	upper := sort.Search(t.processed.Len(), func(i int) bool {
		return t.processed[i].Mean > x
	})

	z1 := x - t.processed[upper-1].Mean
	z2 := t.processed[upper].Mean - x
	return weightedAverage(t.cumulative[upper-1], z2, t.cumulative[upper], z1) / t.processedWeight
}

func weightedAverage(x1, w1, x2, w2 float64) float64 {
	if x1 <= x2 {
		return weightedAverageSorted(x1, w1, x2, w2)
	}
	return weightedAverageSorted(x2, w2, x1, w1)
}

func weightedAverageSorted(x1, w1, x2, w2 float64) float64 {
	x := (x1*w1 + x2*w2) / (w1 + w2)
	return math.Max(x1, math.Min(x, x2))
}

// decayLimit is 0.9**100, maybe configurable?
const decayLimit = 0.00002656139889

// decay decays the histo to make values at the top less interesting over time
// the total digest count will converge to `bufferSize / (1 - decayFactor)`
// if we use `decayFactor` 0.9 and `bufferSize` 1000, this means total count 10000
// so 99th percentile will not be overly influenced by a few bad values
// and similarly the ranking/selection will not be
// (provided we use scale function which keeps small enough bins towards the top)
func (t *TDigest) decay() {
	t.processIt(false) // don't update cumulative as we'll do that below inline
	var weight float64
	var remove []int
	t.cumulative = t.cumulative[:0]
	prev := 0.0

	min := t.processed[0].Mean
	max := t.processed[len(t.processed)-1].Mean

	for i := range t.processed {
		c := &t.processed[i]
		c.Weight *= t.decayValue
		if c.Weight < decayLimit {
			remove = append(remove, i)
		} else {
			weight += c.Weight
			// do cumulative work inline
			t.cumulative = append(t.cumulative, prev+c.Weight/2.0)
			prev += c.Weight
		}
	}
	t.cumulative = append(t.cumulative, prev)

	if len(remove) > 0 {
		for i, c := range remove {
			calculated := c - i
			t.processed = append(t.processed[:calculated], t.processed[calculated+1:]...)
		}
		if len(t.processed) > 0 {
			// only set these if we've removed those centroids
			if min != t.processed[0].Mean {
				t.min = t.processed[0].Mean
			}
			if max != t.processed[len(t.processed)-1].Mean {
				t.max = t.processed[len(t.processed)-1].Mean
			}
		} else {
			t.min = math.Inf(+1)
			t.max = math.Inf(-1)
		}
	}

	t.processedWeight = weight
}

func (t *TDigest) Clone() *TDigest {
	t.process()
	td := &TDigest{
		Compression:       t.Compression,
		maxProcessed:      t.maxProcessed,
		maxUnprocessed:    t.maxUnprocessed,
		processed:         make(CentroidList, 0, t.maxProcessed),
		unprocessed:       make(CentroidList, 0, t.maxUnprocessed+1),
		cumulative:        make([]float64, 0, t.maxUnprocessed+1),
		processedWeight:   t.processedWeight,
		unprocessedWeight: t.unprocessedWeight,
		min:               t.min,
		max:               t.max,
		count:             t.count,
		decayCount:        t.decayCount,
		decayEvery:        t.decayEvery,
		decayValue:        t.decayValue,
		Scaler:            t.Scaler,
	}

	td.processed = append(td.processed, t.processed...)
	td.cumulative = append(td.cumulative, t.cumulative...)
	// we've processed so unprocessed will be empty

	return td
}

// MarshalBinary serializes d as a sequence of bytes, suitable to be
// deserialized later with UnmarshalBinary.
func (t *TDigest) MarshalBinary() ([]byte, error) {
	t.process()
	return marshalBinary(t)
}

// UnmarshalBinary populates d with the parsed contents of p, which should have
// been created with a call to MarshalBinary.
func (t *TDigest) UnmarshalBinary(p []byte) error {
	return unmarshalBinary(t, p)
}

func (t *TDigest) Count() int64 {
	return t.count
}

func (t *TDigest) Min() float64 {
	return t.min
}

func (t *TDigest) Max() float64 {
	return t.max
}

func (t *TDigest) TotalWeight() float64 {
	return t.processedWeight
}

func (t *TDigest) CheckWeights() int {
	t.process()
	return t.checkWeights(t.processed, t.processedWeight)
}

func (t *TDigest) checkWeights(w CentroidList, total float64) int {
	badCount := 0
	n := len(w) - 1
	if w[n].Weight > 0 {
		n++
	}

	normalizer := t.Scaler.normalizer(t.Compression, t.processedWeight)
	k1 := t.Scaler.k(0, normalizer)
	q := float64(0)
	left := float64(0)
	header := "\n"
	for i := 0; i < n; i++ {
		dq := w[i].Weight / total
		k2 := t.Scaler.k(q+dq, normalizer)
		q += dq / 2
		if k2-k1 > 1 && w[i].Weight != 1 {
			fmt.Printf("%sOversize centroid at "+
				"%d, k0=%.2f, k1=%.2f, dk=%.2f, w=%.2f, q=%.4f, dq=%.4f, left=%.1f, current=%.2f maxw=%.2f\n",
				header, i, k1, k2, k2-k1, w[i], q, dq, left, w[i], t.processedWeight*t.Scaler.max(q, normalizer))
			header = ""
			badCount++
		}
		if k2-k1 > 4 && w[i].Weight != 1 {
			panic(
				fmt.Sprintf("Egregiously oversized centroid at "+
					"%d, k0=%.2f, k1=%.2f, dk=%.2f, w=%.2f, q=%.4f, dq=%.4f, left=%.1f, current=%.2f, maxw=%.2f\n",
					i, k1, k2, k2-k1, w[i], q, dq, left, w[i], t.processedWeight*t.Scaler.max(q, normalizer)))
		}
		q += dq / 2
		left += w[i].Weight
		k1 = k2
	}

	return badCount
}
