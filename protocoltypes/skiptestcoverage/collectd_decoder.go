package skiptestcoverage

import (
	"errors"
	"fmt"
	"io"

	"github.com/cep21/megajson/scanner"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
)

// CollectdJSONWriteFormatJSONDecoder is the struct that decodes CollectDJson format
type CollectdJSONWriteFormatJSONDecoder struct {
	s scanner.Scanner
}

// NewCollectdJSONWriteFormatJSONDecoder creates a new CollectdJSONWriteFormatJSONDecoder
func NewCollectdJSONWriteFormatJSONDecoder(r io.Reader) *CollectdJSONWriteFormatJSONDecoder {
	return &CollectdJSONWriteFormatJSONDecoder{s: scanner.NewScanner(r)}
}

// NewCollectdJSONWriteFormatJSONScanDecoder creates a new CollectdJSONWriteFormatJSONDecoder
func NewCollectdJSONWriteFormatJSONScanDecoder(s scanner.Scanner) *CollectdJSONWriteFormatJSONDecoder {
	return &CollectdJSONWriteFormatJSONDecoder{s: s}
}

// Decode will decode into ptr a JSON object
func (e *CollectdJSONWriteFormatJSONDecoder) Decode(ptr **protocoltypes.CollectdJSONWriteFormat) error {
	s := e.s
	if tok, tokval, err := s.Scan(); err != nil {
		return err
	} else if tok == scanner.TNULL {
		*ptr = nil
		return nil
	} else if tok != scanner.TLBRACE {
		return fmt.Errorf("Unexpected %s at %d: %s; expected '{'", scanner.TokenName(tok), s.Pos(), string(tokval))
	}

	// Create the object if it doesn't exist.
	if *ptr == nil {
		*ptr = &protocoltypes.CollectdJSONWriteFormat{}
	}
	v := *ptr

	// Loop over key/value pairs.
	index := 0
	for {
		// Read in key.
		var key string
		tok, tokval, err := s.Scan()
		if err != nil {
			return err
		} else if tok == scanner.TRBRACE {
			return nil
		} else if tok == scanner.TCOMMA {
			if index == 0 {
				return fmt.Errorf("Unexpected comma at %d", s.Pos())
			}
			if tok, tokval, err = s.Scan(); err != nil {
				return err
			}
		}

		if tok != scanner.TSTRING {
			return fmt.Errorf("Unexpected %s at %d: %s; expected '{' or string", scanner.TokenName(tok), s.Pos(), string(tokval))
		}
		key = string(tokval)

		// Read in the colon.
		if tok, tokval, err := s.Scan(); err != nil {
			return err
		} else if tok != scanner.TCOLON {
			return fmt.Errorf("Unexpected %s at %d: %s; expected colon", scanner.TokenName(tok), s.Pos(), string(tokval))
		}

		switch key {

		case "dsnames":
			v := &v.Dsnames

			if err := NewstringJSONScanDecoder(s).DecodeArray(v); err != nil {
				return err
			}

		case "dstypes":
			v := &v.Dstypes

			if err := NewstringJSONScanDecoder(s).DecodeArray(v); err != nil {
				return err
			}

		case "host":
			v := &v.Host

			if err := NewstringJSONScanDecoder(s).Decode(v); err != nil {
				return err
			}

		case "interval":
			v := &v.Interval

			if err := Newfloat64JSONScanDecoder(s).Decode(v); err != nil {
				return err
			}

		case "plugin":
			v := &v.Plugin

			if err := NewstringJSONScanDecoder(s).Decode(v); err != nil {
				return err
			}

		case "plugin_instance":
			v := &v.PluginInstance

			if err := NewstringJSONScanDecoder(s).Decode(v); err != nil {
				return err
			}

		case "time":
			v := &v.Time

			if err := Newfloat64JSONScanDecoder(s).Decode(v); err != nil {
				return err
			}

		case "type":
			v := &v.TypeS

			if err := NewstringJSONScanDecoder(s).Decode(v); err != nil {
				return err
			}

		case "type_instance":
			v := &v.TypeInstance

			if err := NewstringJSONScanDecoder(s).Decode(v); err != nil {
				return err
			}

		case "values":
			v := &v.Values

			if err := Newfloat64JSONScanDecoder(s).DecodeArray(v); err != nil {
				return err
			}

		}

		index++
	}
}

// DecodeArray will decode into ptr an array of JSON object
func (e *CollectdJSONWriteFormatJSONDecoder) DecodeArray(ptr *[]*protocoltypes.CollectdJSONWriteFormat) error {
	s := e.s
	if tok, _, err := s.Scan(); err != nil {
		return err
	} else if tok != scanner.TLBRACKET {
		return errors.New("Expected '['")
	}

	var slice []*protocoltypes.CollectdJSONWriteFormat
	// Loop over items.
	index := 0
	for {
		tok, tokval, err := s.Scan()
		if err != nil {
			return err
		} else if tok == scanner.TRBRACKET {
			*ptr = slice
			return nil
		} else if tok == scanner.TCOMMA {
			if index == 0 {
				return fmt.Errorf("Unexpected comma in array at %d", s.Pos())
			}
			if tok, tokval, err = s.Scan(); err != nil {
				return err
			}
		}
		s.Unscan(tok, tokval)

		item := &protocoltypes.CollectdJSONWriteFormat{}
		if err := e.Decode(&item); err != nil {
			return err
		}
		slice = append(slice, item)

		index++
	}
}
