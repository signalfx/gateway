package zipper

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
)

// ReadZipper creates a Pool that contains previously used Readers and can create new ones if we run out.
type ReadZipper struct {
	zippers    sync.Pool
	Count      int64
	ErrorCount int64
}

// GzipHandler transparently decodes your possibly gzipped request
func (z *ReadZipper) GzipHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if "gzip" == r.Header.Get("Content-Encoding") {
			gzi := z.zippers.Get()
			if gzi != nil {
				gz := gzi.(*gzip.Reader)
				// put it back
				defer z.zippers.Put(gz)
				err := gz.Reset(r.Body)
				if err == nil {
					err = gz.Close()
					if err == nil {
						// nasty? could construct another object but seems expensive
						r.Body = gz
						h.ServeHTTP(w, r)
						return
					}
				}
			}
		}
		h.ServeHTTP(w, r)
	})
}

// NewZipper gives you a ReadZipper
func NewZipper() *ReadZipper {
	return newZipper(gzip.NewReader)
}

func newZipper(zipperFunc func(r io.Reader) (*gzip.Reader, error)) *ReadZipper {
	z := &ReadZipper{}
	z.zippers = sync.Pool{New: func() interface{} {
		atomic.AddInt64(&z.Count, 1)
		// This is just the header of an empty gzip, unlike NewWriter, i can't pass in nil ot empty bytes
		g, err := zipperFunc(bytes.NewBuffer([]byte{31, 139, 8, 0, 0, 0, 0, 0, 0, 255}))
		if err == nil {
			return g
		}
		return nil
	}}
	return z
}
