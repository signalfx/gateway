package jsonengines

import (
	"encoding/json"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
	"github.com/signalfuse/signalfxproxy/protocoltypes/skiptestcoverage"
	"io"
)

// JSONDecodingEngine creates a JSON decoder
type JSONDecodingEngine interface {
	// DecodeCollectdJSONWriteBody decodes json type CollectdJSONWriteBody
	DecodeCollectdJSONWriteBody(readCloser io.Reader) (protocoltypes.CollectdJSONWriteBody, error)
}

// NativeMarshallJSONDecoder uses golang's built in json decoder
type NativeMarshallJSONDecoder struct {
}

// DecodeCollectdJSONWriteBody decodes json type CollectdJSONWriteBody
func (decoder *NativeMarshallJSONDecoder) DecodeCollectdJSONWriteBody(readCloser io.Reader) (protocoltypes.CollectdJSONWriteBody, error) {
	var postFormatNative protocoltypes.CollectdJSONWriteBody
	return postFormatNative, json.NewDecoder(readCloser).Decode(&postFormatNative)
}

// MegaJSONJSONDecoder uses the megajson package to decode json
type MegaJSONJSONDecoder struct {
}

// DecodeCollectdJSONWriteBody decodes json type CollectdJSONWriteBody
func (decoder *MegaJSONJSONDecoder) DecodeCollectdJSONWriteBody(readCloser io.Reader) (protocoltypes.CollectdJSONWriteBody, error) {
	var postFormatNative protocoltypes.CollectdJSONWriteBody
	return postFormatNative, skiptestcoverage.NewCollectdJSONWriteFormatJSONDecoder(readCloser).DecodeArray((*[]*protocoltypes.CollectdJSONWriteFormat)(&postFormatNative))
}
