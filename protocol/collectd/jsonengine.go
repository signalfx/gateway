package collectd

import (
	"encoding/json"
	"fmt"
	"io"
)

// JSONEngine creates a JSON decoder
type JSONEngine interface {
	// DecodeJSONWriteBody decodes json type JSONWriteBody
	DecodeJSONWriteBody(readCloser io.Reader) (JSONWriteBody, error)
}

// NativeMarshallJSONDecoder uses golang's built in json decoder
type NativeMarshallJSONDecoder struct {
}

// DecodeJSONWriteBody decodes json type JSONWriteBody
func (decoder *NativeMarshallJSONDecoder) DecodeJSONWriteBody(readCloser io.Reader) (JSONWriteBody, error) {
	var postFormatNative JSONWriteBody
	return postFormatNative, json.NewDecoder(readCloser).Decode(&postFormatNative)
}

var knownEngines = map[string]JSONEngine{
	"":       &NativeMarshallJSONDecoder{},
	"native": &NativeMarshallJSONDecoder{},
}

// LoadEngine will load a Engine of the given name, or return an error if the engine doens't exist
func LoadEngine(name string) (JSONEngine, error) {
	engine, exists := knownEngines[name]
	if !exists {
		return nil, fmt.Errorf("Unable to load json engine by the name of %s", name)
	}
	return engine, nil
}
