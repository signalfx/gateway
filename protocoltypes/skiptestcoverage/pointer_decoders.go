package skiptestcoverage

// Should be in same package as collectd_decoder.go
import (
	"fmt"
	"github.com/cep21/megajson/scanner"
)

// StringPointerDecoder decodes pointers to strings using megajson
type StringPointerDecoder struct {
	s scanner.Scanner
}

// NewstringJSONScanDecoder creates a new StringPointerDecoder
func NewstringJSONScanDecoder(s scanner.Scanner) *StringPointerDecoder {
	return &StringPointerDecoder{
		s: s,
	}
}

// Decode decodes a JSON pointer to a string
func (decoder *StringPointerDecoder) Decode(s **string) error {
	*s = new(string)
	return decoder.s.ReadString(*s)
}

// DecodeArray decodes an array of JSON pointer to a string
func (decoder *StringPointerDecoder) DecodeArray(s *[]*string) error {
	r := make([]interface{}, 0, 3)
	err := decoder.s.ReadArray(&r)
	if err != nil {
		return err
	}
	*s = make([]*string, len(r), len(r))
	for i := range r {
		str, ok := r[i].(string)
		if ok {
			(*s)[i] = &str
		} else {
			return fmt.Errorf("Unknown type for %s", r[i])
		}
	}
	return nil
}

// Float64PointDecoder decodes pointers to float64 using megajson
type Float64PointDecoder struct {
	s scanner.Scanner
}

// Newfloat64JSONScanDecoder creates a new float64 megajson decoder
func Newfloat64JSONScanDecoder(s scanner.Scanner) *Float64PointDecoder {
	return &Float64PointDecoder{
		s: s,
	}
}

// Decode into a pointer of a float a float value from JSON
func (decoder *Float64PointDecoder) Decode(s **float64) error {
	*s = new(float64)
	return decoder.s.ReadFloat64(*s)
}

// DecodeArray into a pointer of an array an array of float value from JSON
func (decoder *Float64PointDecoder) DecodeArray(s *[]*float64) error {
	r := make([]interface{}, 0, 3)
	err := decoder.s.ReadArray(&r)
	if err != nil {
		return err
	}
	*s = make([]*float64, len(r), len(r))
	for i := range r {
		str, ok := r[i].(float64)
		if ok {
			(*s)[i] = &str
		} else {
			return fmt.Errorf("Unknown type for %s", r[i])
		}

	}
	return nil
}
