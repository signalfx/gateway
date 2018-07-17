package bloomfilter

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/spaolacci/murmur3"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"
)

var (
	wordlist1low  []uint64
	wordlist1high []uint64
	n             = 1 << 16
	bf            *Bloom
	hasher        murmur3.Hash128
)

func doHash(data []byte) (uint64, uint64) {
	hasher.Reset()
	hasher.Write(data)
	return hasher.Sum128()
}

func TestMain(m *testing.M) {
	hasher = murmur3.New128()
	file, err := os.Open("/usr/share/dict/words")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	wordlist1low = make([]uint64, n)
	wordlist1high = make([]uint64, n)
	for i := range wordlist1low {
		if scanner.Scan() {
			low, high := doHash([]byte(scanner.Text()))
			wordlist1low[i] = low
			wordlist1high[i] = high
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Print("############\nBenchmarks relate to 2**16 OP. --> so divide op/ns by 65536\n###############\n\n")

	m.Run()
}

func TestM_NumberOfWrongs(t *testing.T) {
	bf = New(1048576, 0.007812500000000009)
	cnt := 0
	for i := range wordlist1low {
		if !bf.AddIfNotHas(wordlist1low[i], wordlist1high[i]) {
			cnt++
		}
	}
	fmt.Printf("Bloomfilter New(10 * 2**16, 7) (-> size=%v bit): \n            Check for 'false positives': %v wrong positive 'Has' results on 2**16 entries => %v %%\n", len(bf.bitset)<<6, cnt, float64(cnt)/float64(n))
}

func Test_New(t *testing.T) {
	Convey("Calling New()", t, func() {
		Convey("With a size and false positive pct", func() {
			bloom := New(1000, float64(.007))
			Convey("Sets the number of locations based on input", func() {
				So(bloom.setLocs, ShouldEqual, 8)
				capacity, fpp := bloom.Size()
				So(capacity, ShouldEqual, 1000)
				So(fpp, ShouldEqual, .007)
			})
		})
		Convey("With a size and false positive pct1", func() {
			bloom := New(10*(1<<16), float64(0.007812500000000009))
			Convey("Sets the number of locations based on input", func() {
				So(bloom.setLocs, ShouldEqual, 7)
			})
		})
		Convey("With a calculated size < 512", func() {
			bloom := New(1, float64(.007))
			Convey("Sets the size to 511", func() {
				So(bloom.size, ShouldEqual, 511)
			})
		})
	})
}

func Test_Has(t *testing.T) {
	Convey("With a bloom filter with one entry in it", t, func() {
		bloom := New(1000, float64(.007))
		keyLow, keyHigh := doHash([]byte("foo"))
		bloom.Add(keyLow, keyHigh)
		Convey("Has returns true for the same entry", func() {
			So(bloom.Has(keyLow, keyHigh), ShouldBeTrue)
		})
		Convey("Has returns false for the another entry", func() {
			So(bloom.Has(doHash([]byte("foo1"))), ShouldBeFalse)
		})
	})
}

func Test_AddIfNotHas(t *testing.T) {
	Convey("With a bloom filter with one entry in it", t, func() {
		bloom := New(1000, float64(.007))
		keyLow, keyHigh := doHash([]byte("foo"))
		keyLowExisting, keyHighExisting := doHash([]byte("foo-exists"))
		bloom.Add(keyLowExisting, keyHighExisting)
		Convey("AddIfNotHas returns true when a new entry is added", func() {
			So(bloom.AddIfNotHas(keyLow, keyHigh), ShouldBeTrue)
			So(bloom.ElemNum, ShouldEqual, 2)
			So(bloom.BitNum, ShouldEqual, bloom.setLocs*2)
		})
		Convey("AddIfNotHas returns false for the second add of the existing key", func() {
			So(bloom.AddIfNotHas(keyLowExisting, keyHighExisting), ShouldBeFalse)
			So(bloom.ElemNum, ShouldEqual, 1)
			So(bloom.BitNum, ShouldEqual, bloom.setLocs)
		})
	})
}

func Test_Clear(t *testing.T) {
	Convey("With a bloom filter with one entry in it", t, func() {
		bloom := New(1000, float64(.007))
		keyLow, keyHigh := doHash([]byte("foo"))
		bloom.Add(keyLow, keyHigh)
		Convey("Filter is empty after calling Clear()", func() {
			bloom.Clear()
			So(bloom.Has(keyLow, keyHigh), ShouldBeFalse)
			So(bloom.ElemNum, ShouldEqual, 0)
			So(bloom.BitNum, ShouldEqual, 0)
		})
	})
}

func TestBloom_ExpectedFpp(t *testing.T) {
	Convey("With a bloom filter with one entry in it", t, func() {
		bloom := newWithFixedSize(1024, 8)
		keyLow, keyHigh := doHash([]byte("foo"))
		bloom.Add(keyLow, keyHigh)
		Convey("ExpectedFpp() should reflect current fpp", func() {
			So(bloom.ExpectedFpp(), ShouldEqual, 1.3877787807814457e-17)
			Convey("Adding another element should change current fpp", func() {
				keyLow, keyHigh = doHash([]byte("foo1"))
				bloom.Add(keyLow, keyHigh)
				So(bloom.ExpectedFpp(), ShouldEqual, 3.552713678800501e-15)
			})
			Convey("Adding the same element should not change current fpp", func() {
				bloom.Add(keyLow, keyHigh)
				So(bloom.ExpectedFpp(), ShouldEqual, 1.3877787807814457e-17)
			})
		})
	})
}

type errorer struct{}

func (e *errorer) Write(p []byte) (int, error) {
	return 0, errors.New("no sir")
}

func (e *errorer) Read(p []byte) (int, error) {
	return 0, errors.New("no sir")
}

func TestSerializeDeSerialize(t *testing.T) {
	Convey("With a bloom filter with one entry in it", t, func() {
		bloom := newWithFixedSize(1024, 8)
		keyLow, keyHigh := doHash([]byte("foo"))
		bloom.Add(keyLow, keyHigh)
		buff := make([]byte, 65536)
		Convey("We can write the bloom", func() {
			outfile, err := ioutil.TempFile("", "testing")
			So(err, ShouldBeNil)
			err = bloom.Write(outfile, buff)
			So(err, ShouldBeNil)
			outfile.Close()
			b := &bytes.Buffer{}
			err = bloom.Write(b, buff)
			So(err, ShouldBeNil)
			Convey("And we can read it back", func() {
				newbloom, err := NewFromFile(outfile.Name())
				So(err, ShouldBeNil)
				So(newbloom.BitNum, ShouldEqual, bloom.BitNum)
				So(newbloom.ElemNum, ShouldEqual, bloom.ElemNum)
				So(newbloom.size, ShouldEqual, bloom.size)
				So(newbloom.setLocs, ShouldEqual, bloom.setLocs)
				So(len(newbloom.bitset), ShouldEqual, len(bloom.bitset))
				for i := 0; i < len(newbloom.bitset); i++ {
					So(newbloom.bitset[i], ShouldEqual, bloom.bitset[i])
				}
				So(newbloom, ShouldResemble, bloom)
			})
			Convey("Read can fail", func() {
				b := bytes.NewBuffer(b.Bytes()[:52])
				newbloom := &Bloom{}
				err := newbloom.Read(b, buff)
				So(err, ShouldNotBeNil)
			})
			Convey("Wrong version fail", func() {
				bb := make([]byte, 4)
				b := bytes.NewBuffer(bb)
				newbloom := &Bloom{}
				err := newbloom.Read(b, buff)
				So(err, ShouldEqual, errWrongVersion)
			})
		})
		Convey("bloom write returns error", func() {
			So(bloom.Write(&errorer{}, buff), ShouldNotBeNil)
		})
		Convey("bloom read returns error", func() {
			So(bloom.Read(&errorer{}, buff), ShouldNotBeNil)
		})
	})
}

func BenchmarkM_New(b *testing.B) {
	for r := 0; r < b.N; r++ {
		_ = New(uint64(n*10), float64(.007))
	}
}

func BenchmarkM_Clear(b *testing.B) {
	bf = newWithFixedSize(uint64(n*10), 7)
	for i := range wordlist1low {
		bf.Add(wordlist1low[i], wordlist1high[i])
	}
	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		bf.Clear()
	}
}

func BenchmarkM_Add(b *testing.B) {
	bf = newWithFixedSize(uint64(n*10), 7)
	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1low {
			bf.Add(wordlist1low[i], wordlist1high[i])
		}
	}

}

func BenchmarkM_Has(b *testing.B) {
	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1low {
			bf.Has(wordlist1low[i], wordlist1high[i])
		}
	}

}

func BenchmarkM_AddIfNotHasFALSE(b *testing.B) {
	bf = newWithFixedSize(uint64(n*10), 7)
	for i := range wordlist1low {
		bf.Add(wordlist1low[i], wordlist1high[i])
	}
	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1low {
			bf.AddIfNotHas(wordlist1low[i], wordlist1high[i])
		}
	}
}

func BenchmarkM_AddIfNotHasClearTRUE(b *testing.B) {
	bf = newWithFixedSize(uint64(n*10), 7)

	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		for i := range wordlist1low {
			bf.AddIfNotHas(wordlist1low[i], wordlist1high[i])
		}
		bf.Clear()
	}
}

func BenchmarkBloom_Read(b *testing.B) {
	bf = newWithFixedSize(uint64(n*10), 7)
	tmpdir, _ := ioutil.TempDir(os.TempDir(), "bloom")
	file, _ := os.Create(path.Join(tmpdir, "bloom"))
	buff := make([]byte, 65536)
	bf.Write(file, buff)
	file.Close()
	readInto := Bloom{}

	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		file, _ := os.Open(path.Join(tmpdir, "bloom"))
		readInto.Read(file, buff)
		file.Close()
	}
}

func BenchmarkBloom_Write(b *testing.B) {
	bf = newWithFixedSize(uint64(n*10), 7)
	tmpdir, _ := ioutil.TempDir(os.TempDir(), "bloom")
	buff := make([]byte, 65536)

	b.ResetTimer()
	for r := 0; r < b.N; r++ {
		file, _ := os.Open(path.Join(tmpdir, "bloom"))
		bf.Write(file, buff)
		file.Close()
	}
}
