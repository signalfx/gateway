package signalfx

import (
	"context"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/trace"
	"github.com/signalfx/golib/web"
	"net/http"
)

const (
	// DefaultTracePathV1 is the default listen path
	DefaultTracePathV1 = "/v1/trace"
	// ZipkinV1 is a constant used for protocol naming
	ZipkinV1 = "zipkin_json_v1"
)

// Dataformat is used to read the json off the wire
type Dataformat []*trace.Span

// InputSpan defines a span that is the union of v1 and v2 spans
type InputSpan struct {
	trace.Span
	Annotations       []Annotation       `json:"annotations"`
	BinaryAnnotations []BinaryAnnotation `json:"binaryAnnotations"`
}

// Annotation associates an event that explains latency with a timestamp.
// Unlike log statements, annotations are often codes. Ex. “ws” for WireSend
type Annotation struct {
	Endpoint  *trace.Endpoint `json:"endpoint"`
	Timestamp *float64        `json:"timestamp"`
	Value     *string         `json:"value"`
}

// BinaryAnnotation associates an event that explains latency with a timestamp.
type BinaryAnnotation struct {
	Endpoint *trace.Endpoint `json:"endpoint"`
	Key      *string         `json:"key"`
	Value    *interface{}    `json:"value"`
}

// JSONTraceDecoderV1 decodes json to structs
type JSONTraceDecoderV1 struct {
	Logger log.Logger
	Sink   trace.Sink
}

var errInvalidJSONTraceFormat = errors.New("invalid JSON format; please see correct format at https://zipkin.io/zipkin-api/#/default/post_spans")

// Read the data off the wire in json format
func (decoder *JSONTraceDecoderV1) Read(ctx context.Context, req *http.Request) error {
	dec := json.NewDecoder(req.Body)
	var input Dataformat
	if err := dec.Decode(&input); err != nil {
		return errInvalidJSONTraceFormat
	}
	if len(input) == 0 {
		return nil
	}
	return decoder.Sink.AddSpans(ctx, input)
}

func setupJSONTraceV1(ctx context.Context, r *mux.Router, sink Sink, logger log.Logger, httpChain web.NextConstructor) sfxclient.Collector {
	handler, st := setupChain(ctx, sink, ZipkinV1, func(s Sink) ErrorReader {
		return &JSONTraceDecoderV1{Logger: logger, Sink: sink}
	}, httpChain, logger)
	SetupJSONByPaths(r, handler, DefaultTracePathV1)
	return st
}
