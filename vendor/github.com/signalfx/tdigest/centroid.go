package tdigest

import (
	"bytes"
	"fmt"
	"math"
	"sort"
)

// ErrWeightLessThanZero is used when the weight is not able to be processed.
const ErrWeightLessThanZero = Error("centroid weight cannot be less than zero")

// Error is a domain error encountered while processing tdigests
type Error string

func (e Error) Error() string {
	return string(e)
}

// Centroid average position of all points in a shape
type Centroid struct {
	Mean   float64
	Weight float64
}

func (c *Centroid) String() string {
	return fmt.Sprintf("{mean: %f weight: %f}", c.Mean, c.Weight)
}

// Add averages the two centroids together and update this centroid
func (c *Centroid) Add(r Centroid) error {
	if r.Weight < 0 {
		return ErrWeightLessThanZero
	}
	if c.Weight != 0 {
		c.Weight += r.Weight
		c.Mean += r.Weight * (r.Mean - c.Mean) / c.Weight
	} else {
		c.Weight = r.Weight
		c.Mean = r.Mean
	}
	return nil
}

// CentroidList is sorted by the Mean of the centroid, ascending.
type CentroidList []Centroid

func (l CentroidList) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	w := &binaryBufferWriter{buf: buf}
	w.writeValue(centroidmagic)
	w.writeValue(encodingVersion)
	w.writeValue(int32(l.Len()))
	for _, c := range l {
		w.writeValue(c.Mean)
		w.writeValue(c.Weight)
	}
	if w.err != nil {
		return nil, w.err
	}
	return buf.Bytes(), nil
}

func UnmarshalCentroidListIntoTdigest(p []byte, digest *TDigest) error {
	var (
		mv int16
		ev int32
		n  int32
	)
	r := &binaryReader{r: bytes.NewReader(p)}
	r.readValue(&mv)
	if r.err != nil {
		return r.err
	}
	if mv != centroidmagic {
		return fmt.Errorf("data corruption detected: invalid header magic value 0x%04x", mv)
	}
	r.readValue(&ev)
	if r.err != nil {
		return r.err
	}
	if ev != encodingVersion {
		return fmt.Errorf("data corruption detected: invalid encoding version %d", ev)
	}
	r.readValue(&n)
	var c Centroid
	for i := 0; i < int(n); i++ {
		r.readValue(&c.Mean)
		r.readValue(&c.Weight)
		if r.err != nil {
			return r.err
		}
		if c.Weight < 0 {
			return fmt.Errorf("data corruption detected: negative count: %f", c.Weight)
		}
		if math.IsNaN(c.Mean) {
			return fmt.Errorf("data corruption detected: NaN mean not permitted")
		}
		if math.IsInf(c.Mean, 0) {
			return fmt.Errorf("data corruption detected: Inf mean not permitted")
		}
		digest.AddCentroid(c)
	}
	if r.err != nil {
		return r.err
	}
	return nil
}

func UnmarshalCentroidBinary(p []byte) (CentroidList, error) {
	var (
		mv int16
		ev int32
		n  int32
	)
	r := &binaryReader{r: bytes.NewReader(p)}
	r.readValue(&mv)
	if r.err != nil {
		return nil, r.err
	}
	if mv != centroidmagic {
		return nil, fmt.Errorf("data corruption detected: invalid header magic value 0x%04x", mv)
	}
	r.readValue(&ev)
	if r.err != nil {
		return nil, r.err
	}
	if ev != encodingVersion {
		return nil, fmt.Errorf("data corruption detected: invalid encoding version %d", ev)
	}
	r.readValue(&n)
	l := make(CentroidList, n)
	for i := 0; i < int(n); i++ {
		c := Centroid{}
		r.readValue(&c.Mean)
		r.readValue(&c.Weight)
		if r.err != nil {
			return nil, r.err
		}
		if c.Weight < 0 {
			return nil, fmt.Errorf("data corruption detected: negative count: %f", c.Weight)
		}
		if math.IsNaN(c.Mean) {
			return nil, fmt.Errorf("data corruption detected: NaN mean not permitted")
		}
		if math.IsInf(c.Mean, 0) {
			return nil, fmt.Errorf("data corruption detected: Inf mean not permitted")
		}
		l[i] = c
	}
	if r.err != nil {
		return nil, r.err
	}
	return l, nil
}

func (l *CentroidList) Clear() {
	*l = (*l)[0:0]
}

func (l CentroidList) Len() int {
	return len(l)
}
func (l CentroidList) Less(i, j int) bool { return l[i].Mean < l[j].Mean }
func (l CentroidList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

// NewCentroidList creates a priority queue for the centroids
func NewCentroidList(centroids []Centroid) CentroidList {
	l := CentroidList(centroids)
	sort.Sort(l)
	return l
}
