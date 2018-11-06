package encoding

import (
	"github.com/signalfx/golib/trace"
	"time"
)

// SpanIdentity is a tuple of Service and Operation
//easyjson:json
type SpanIdentity struct {
	Service    string `json:",omitempty"`
	Operation  string `json:",omitempty"`
	Error      bool   `json:",omitempty"`
	HttpMethod string `json:",omitempty"`
	Kind       string `json:",omitempty"`
}

func (k *SpanIdentity) Dims() map[string]string {
	m := map[string]string{
		"service":   k.Service,
		"operation": k.Operation,
	}
	if k.Error {
		m["error"] = "true"
	} else {
		m["error"] = "false"
	}
	if k.HttpMethod != "" {
		m["httpMethod"] = k.HttpMethod
	}
	if k.Kind != "" {
		m["kind"] = k.Kind
	}
	return m
}

//easyjson:json
type HistoOnDiskEntry struct {
	Digest       []byte    `json:",omitempty"`
	Last         time.Time `json:",omitempty"`
	Count        int64     `json:",omitempty"`
	DecayedCount float64   `json:",omitempty"`
}

//easyjson:json
type HistoOnDisk struct {
	Entries              map[SpanIdentity]HistoOnDiskEntry `json:",omitempty"`
	MetricsReservoirSize int                               `json:",omitempty"`
	MetricsAlphaFactor   float64                           `json:",omitempty"`
}

//easyjson:json
type ExpiredBufferEntry struct {
	BufferEntry
	NewSpanSeen bool `json:",omitempty"` // we've seen a new span
	Released    bool `json:",omitempty"`
}

//easyjson:json
type ExpiredBufferEntries []*ExpiredBufferEntry

//easyjson:json
type BufferEntry struct {
	Spans         []*trace.Span `json:",omitempty"` // buffer of spans by trace id
	Last          time.Time     `json:",omitempty"` // Last time we saw a span for this trace id
	LatestEndTime float64       `json:",omitempty"` // Latest end time we've seen for any span
	StartTime     float64       `json:",omitempty"` // Start time of initiating span if found
	Initiating    *trace.Span   `json:",omitempty"` // initiating span
	ToBeReleased  bool          `json:",omitempty"` // spans that have been selected to be released
}

//easyjson:json
type BufferEntries []*BufferEntry

//easyjson:json
type BufferOnDisk struct {
	Traces        map[string]*BufferEntry        `json:",omitempty"` // map of trace id to buffer entry
	NumSpans      int64                          `json:",omitempty"` // num spans buffered in Traces
	ExpiredTraces map[string]*ExpiredBufferEntry `json:",omitempty"` // map of trace id to expired buffer entry
}

//easyjson:json
type SampleList []*SampleEntry

//easyjson:json
type SampleEntry struct {
	ID      *SpanIdentity `json:",omitempty"`
	Samples []int64      `json:",omitempty"`
}

//easyjson:json
type EtcdConfig struct {
	RebalanceAddress *string `json:",omitempty"`
	IngestAddress    *string `json:",omitempty"`
	ID               *string `json:",omitempty"`
}
