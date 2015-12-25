package log

import (
	"bytes"
	"encoding/json"
	"github.com/signalfx/golib/errors"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

type panicError struct{}

func (p *panicError) Error() string {
	panic("failure?")
}

func TestJSONLogger(t *testing.T) {
	Convey("JSON logger to a buffer", t, func() {
		b := &bytes.Buffer{}
		l := NewJSONLogger(b, Panic)
		Convey("should convert non string keys", func() {
			l.Log(5678, 1234)
			So(b.String(), ShouldContainSubstring, "1234")
			So(b.String(), ShouldContainSubstring, "5678")
		})
		Convey("should throw normal panics", func() {
			So(func() {
				l.Log("err", &panicError{})
			}, ShouldPanicWith, "failure?")

		})
		Convey("should forward errors", func() {
			c := NewChannelLogger(1, Panic)
			l := NewJSONLogger(b, c)
			l.Log("type", func() {})
			err := errors.Tail(<-c.Err)
			_, ok := err.(*json.UnsupportedTypeError)
			So(ok, ShouldBeTrue)
		})
	})
}
