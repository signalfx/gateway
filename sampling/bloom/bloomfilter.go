package bloomfilter

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"sync/atomic"
)

// ed0 is the closest i can get to tedo
const version = 0xed0

// helper
func getSize(ui64 uint64) uint64 {
	if ui64 < uint64(512) {
		ui64 = uint64(512)
	}
	size := uint64(1)
	for size < ui64 {
		size <<= 1
	}
	return size
}

// Optimal size (number of elements in the bit array)
// size = -((numEntries*ln(falsePositiveRate))/(ln(2)^2));
// Optimal number of hash functions
// locs = (size/numEntries) * ln(2);
func calcSizeByFalsePositiveRate(numEntries uint64, falsePositiveRate float64) (uint64, uint64) {
	floatNumEntries := float64(numEntries)
	size := -1 * floatNumEntries * math.Log(falsePositiveRate) / math.Pow(math.Ln2, 2)
	locs := math.Ceil(math.Ln2 * size / floatNumEntries)
	return uint64(size), uint64(locs)
}

// New returns a new bloom filter
func New(capacity uint64, falsePositiveRate float64) *Bloom {
	entries, locs := calcSizeByFalsePositiveRate(capacity, falsePositiveRate)
	ret := newWithFixedSize(entries, int(locs))
	ret.capacity = capacity
	ret.falsePositiveRate = falsePositiveRate
	return ret
}

// NewFromFile gives you a bloom filter from a file
func NewFromFile(loc string) (b *Bloom, err error) {
	var f *os.File
	if f, err = os.Open(loc); err == nil {
		b = &Bloom{}
		buff := make([]byte, 65536)
		err = b.Read(f, buff)
	}
	return b, err
}

func newWithFixedSize(entries uint64, locs int) *Bloom {
	size := int64(getSize(entries))
	bloomfilter := &Bloom{
		size:    size - 1,
		setLocs: locs,
	}
	bloomfilter.setSize(size)
	return bloomfilter
}

// Bloom filter
type Bloom struct {
	ElemNum           uint64
	BitNum            uint64
	bitset            []uint64
	size              int64
	setLocs           int
	capacity          uint64
	falsePositiveRate float64
}

// Size returns the input capacity and false positive rate
func (bl *Bloom) Size() (uint64, float64) {
	return bl.capacity, bl.falsePositiveRate
}

// Add sets the bit(s) for entry; Adds an entry to the Bloom filter
func (bl *Bloom) Add(low, high uint64) {
	combinedHash := int64(low)
	signedHigh := int64(high)
	for i := 0; i < bl.setLocs; i++ {
		idx := (combinedHash & (math.MaxInt64)) & bl.size
		if bl.set(uint64(idx)) {
			bl.BitNum++
		}
		combinedHash += signedHigh
	}
	bl.ElemNum++
}

// Has checks if bit(s) for entry is/are set
// returns true if the entry was added to the Bloom Filter
func (bl *Bloom) Has(low, high uint64) bool {
	combinedHash := int64(low)
	signedHigh := int64(high)
	for i := 0; i < bl.setLocs; i++ {
		idx := (combinedHash & (math.MaxInt64)) & bl.size
		if !bl.isSet(uint64(idx)) {
			return false
		}
		combinedHash += signedHigh
	}
	return true
}

// AddIfNotHas only Adds entry if it's not present in the bloomfilter
// returns true if entry was added
// returns false if entry was already registered in the bloomfilter
func (bl *Bloom) AddIfNotHas(low, high uint64) bool {
	if bl.Has(low, high) {
		return false
	}
	bl.Add(low, high)
	return true
}

// ExpectedFpp returns the expected false positive rate for this bloom filter
func (bl *Bloom) ExpectedFpp() float64 {
	return math.Pow(float64(atomic.LoadUint64(&bl.BitNum))/float64(bl.size+1), float64(bl.setLocs))
}

func (bl *Bloom) setSize(sz int64) {
	bl.bitset = make([]uint64, (sz>>6)+1)
}

// Clear resets the Bloom filter
func (bl *Bloom) Clear() {
	for i := range bl.bitset {
		bl.bitset[i] = 0
	}
	bl.ElemNum = 0
	bl.BitNum = 0
}

// Write the bloom filter
func (bl *Bloom) Write(w io.Writer, buff []byte) error {
	errs := make([]error, 0)
	f := func(i interface{}) {
		if err := binary.Write(w, binary.BigEndian, i); err != nil {
			errs = append(errs, err)
		}
	}
	f(uint32(version))
	f(bl.ElemNum)
	f(bl.BitNum)
	f(bl.size)
	f(int32(bl.setLocs))
	f(bl.capacity)
	f(bl.falsePositiveRate)
	length := len(bl.bitset)
	f(int32(length))
	max := len(buff) / 8
	buff = buff[:max*8]
	var i, j int
	for i < length {
		for j = 0; j < max && i < length; j++ {
			binary.BigEndian.PutUint64(buff[j*8:(j+1)*8], bl.bitset[i])
			i++
		}
		_, err := w.Write(buff[:j*8])
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

var errWrongVersion = errors.New("wrongVersion")

// Read the bloom filter
func (bl *Bloom) Read(r io.Reader, buff []byte) error {
	errs := make([]error, 0)
	e := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	f := func(i interface{}) {
		e(binary.Read(r, binary.BigEndian, i))
	}
	var v uint32
	f(&v)
	if v != version {
		e(errWrongVersion)
	}
	f(&bl.ElemNum)
	f(&bl.BitNum)
	f(&bl.size)
	var length, locs int32
	f(&locs)
	f(&bl.capacity)
	f(&bl.falsePositiveRate)
	bl.setLocs = int(locs)
	f(&length)
	bl.bitset = make([]uint64, int(length))
	max := len(buff) / 8
	buff = buff[:max*8]
	var i, j int
	for i < int(length) {
		n, err := r.Read(buff)
		e(err)
		if n == 0 {
			break
		}
		for j = 0; n > 0 && j < max && i < int(length); j++ {
			bl.bitset[i] = binary.BigEndian.Uint64(buff[j*8 : (j+1)*8])
			i++
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (bl *Bloom) set(idx uint64) bool {
	ret := bl.isSet(idx)
	bl.bitset[idx>>6] |= (1 << (idx % 64))
	return !ret
}

func (bl *Bloom) isSet(idx uint64) bool {
	return bl.bitset[idx>>6]&(1<<(idx%64)) != 0
}
