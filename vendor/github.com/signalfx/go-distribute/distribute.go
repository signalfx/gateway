// Package implements consistent hashing similar to https://github.com/dgryski/go-ketama
// This package is not thread safe, do the locking yourself outside.
// Hash can be a readlock, add and remove a write lock.

package distribute

import (
	"errors"
	"fmt"
	"github.com/spaolacci/murmur3"
	"sort"
)

type bs []Bucket

func (c bs) Less(i, j int) bool { return c[i].Label < c[j].Label }
func (c bs) Len() int           { return len(c) }
func (c bs) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

// Bucket is a label and a weight
type Bucket struct {
	Label  string
	Weight int
}

type continuumPoint struct {
	bucket Bucket
	point  uint
}

// Continuum is a consistent hash ring
type Continuum struct {
	ring     points
	size     int
	replicas int
	buckets  map[string]Bucket
}

type points []continuumPoint

func (c points) Less(i, j int) bool { return c[i].point < c[j].point }
func (c points) Len() int           { return len(c) }
func (c points) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

// use murmur for initial node distribution
func getDigest(in string) []byte {
	h := murmur3.New128()
	h.Write([]byte(in))
	return h.Sum(nil)
}

func hashString(in string) uint {
	// use fast hasher for keys
	h := int32(0)
	for i := 0; i < len(in); i++ {
		h = 31*h + int32(in[i])
	}
	return uint(h) & 0xffffffff
}

// New returns a new hash ring with the number of replicas for each bucket given
func New(buckets []Bucket, replicas int) (*Continuum, error) {
	numbuckets := len(buckets)
	ring, bs, err := getRing(buckets, replicas)

	return &Continuum{
		ring:     ring,
		size:     numbuckets,
		buckets:  bs,
		replicas: replicas,
	}, err
}

func getRing(buckets []Bucket, replicas int) (points, map[string]Bucket, error) {
	if replicas <= 0 {
		return nil, nil, fmt.Errorf("cannot create ring with %d replicas", replicas)
	}
	numbuckets := len(buckets)
	sort.Sort(bs(buckets))
	ring := make(points, 0, numbuckets*replicas*4)
	totalweight := 0
	for _, b := range buckets {
		totalweight += b.Weight
	}
	bs := make(map[string]Bucket, len(buckets))
	for i, b := range buckets {
		pct := float32(b.Weight) / float32(totalweight)

		// this is the equivalent of C's promotion rules, but in Go, to maintain exact compatibility with the C library
		// we want to implement this in c at some point for the lb
		limit := int(float32(float64(pct) * float64(replicas) * float64(numbuckets)))

		for k := 0; k < limit; k++ {
			/* replicas hashes * 4 numbers per hash = points per bucket */
			ss := fmt.Sprintf("%s%d", b.Label, k)
			digest := getDigest(ss)

			for h := 0; h < 4; h++ {
				point := continuumPoint{
					point:  uint(digest[3+h*4])<<24 | uint(digest[2+h*4])<<16 | uint(digest[1+h*4])<<8 | uint(digest[h*4]),
					bucket: buckets[i],
				}
				ring = append(ring, point)
			}
		}
		if _, ok := bs[b.Label]; ok {
			return nil, nil, fmt.Errorf("cannot add multiple buckets with the same label: %s", b.Label)
		}
		bs[b.Label] = b
	}
	sort.Sort(ring)
	return ring, bs, nil
}

// Size gets you the size of the ring
func (c *Continuum) Size() int {
	return c.size
}

// Hash returns you the bucket label for this thing
func (c *Continuum) Hash(thing string) string {
	if len(c.ring) == 0 {
		return ""
	}

	h := hashString(thing)

	// the above md5 is way more expensive than this branch
	var i uint
	i = uint(sort.Search(len(c.ring), func(i int) bool { return c.ring[i].point >= h }))
	if i >= uint(len(c.ring)) {
		i = 0
	}

	return c.ring[i].bucket.Label
}

// Add a node to the ring
func (c *Continuum) Add(label string, weight int) error {
	if _, ok := c.buckets[label]; ok {
		return fmt.Errorf("bucket with label '%s' already exists", label)
	}
	buckets := make([]Bucket, 0, len(c.buckets)+1)
	buckets = append(buckets, Bucket{Label: label, Weight: weight})
	for _, v := range c.buckets {
		buckets = append(buckets, v)
	}
	var err error
	c.ring, c.buckets, err = getRing(buckets, c.replicas)
	c.size = len(buckets)
	return err
}

// Remove a node from the ring
func (c *Continuum) Remove(b string) error {
	if _, ok := c.buckets[b]; !ok {
		return errors.New("this bucket is not part of the ring")
	}
	buckets := make([]Bucket, 0, len(c.buckets)-1)
	for k, v := range c.buckets {
		if k != b {
			buckets = append(buckets, v)
		}
	}
	var err error
	c.ring, c.buckets, err = getRing(buckets, c.replicas)
	c.size = len(buckets)
	return err
}
