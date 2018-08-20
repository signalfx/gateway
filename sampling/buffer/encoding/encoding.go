package encoding

import (
	"github.com/signalfx/golib/trace"
	"time"
)

//easyjson:json
type OnDisk struct {
	Traces   map[string][]*trace.Span `json:",omitempty"` // buffer of spans by trace id
	Last     map[string]time.Time     `json:",omitempty"` // Last time we saw a span for this trace id
	Remember map[string]time.Time     `json:",omitempty"` // cache of traceIDs we keep around when draining ch
	NumSpans int64                    `json:",omitempty"` // num spans buffered in Traces
}
