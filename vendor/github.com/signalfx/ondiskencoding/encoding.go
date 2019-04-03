package encoding

import (
	"errors"
	"fmt"
	"github.com/signalfx/golib/trace"
	"strconv"
	"time"
)

// low, high
type ID [2]uint64

var errInvalid = errors.New("ID is not a 16 or 32 byte hex string")

func GetID(s string) (ID, error) {
	var err error
	var low, high uint64
	if len(s) == 16 {
		low, err = strconv.ParseUint(s, 16, 64)
	} else if len(s) == 32 {
		high, err = strconv.ParseUint(s[:16], 16, 64)
		if err == nil {
			low, err = strconv.ParseUint(s[16:], 16, 64)
		}
	} else {
		err = errInvalid
	}
	return [2]uint64{low, high}, err
}

func (id *ID) String() string {
	if id[1] == 0 {
		return fmt.Sprintf("%016x", id[0])
	}
	return fmt.Sprintf("%016x%016x", id[1], id[0])
}

// SpanIdentity is a tuple of Service and Operation
//easyjson:json
type SpanIdentity struct {
	Service    string `json:",omitempty"`
	Operation  string `json:",omitempty"`
	HttpMethod string `json:",omitempty"`
	Kind       string `json:",omitempty"`
	Error      bool   `json:",omitempty"`
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
type Histo struct {
	Error HistoOnDisk `json:",omitempty"`
	Span  HistoOnDisk `json:",omitempty"`
	Trace HistoOnDisk `json:",omitempty"`
}

//easyjson:json
type ExpiredBufferEntry struct {
	TraceID            ID           `json:",omitempty"` // id of the trace
	Last               int64        `json:",omitempty"` // Last time we saw a span for this trace id
	LatestEndTime      int64        `json:",omitempty"` // Latest end time we've seen for any span
	StartTime          int64        `json:",omitempty"` // EarliestStartTime
	InitiatingIdentity SpanIdentity `json:",omitempty"` // initiating span
	NewSpanSeen        bool         `json:",omitempty"` // we've seen a new span
	Released           bool         `json:",omitempty"` // we've released this trace so send along any new spans we see
	TraceTooLarge      bool         `json:",omitempty"` // trace is too large so increment counters to this effect
	DefinitiveTraceID  bool         `json:",omitempty"` // spanID == traceID for buffer entry
}

func (e *ExpiredBufferEntry) HasInitiating() bool {
	return e.InitiatingIdentity.Service != "" && e.InitiatingIdentity.Operation != ""
}

//easyjson:json
type ExpiredBufferEntries []*ExpiredBufferEntry

//easyjson:json
type BufferEntry struct {
	TraceID        ID          `json:",omitempty"` // id of the trace
	Last           int64       `json:",omitempty"` // Last time we saw a span for this trace id
	LatestEndTime  int64       `json:",omitempty"` // Latest end time we've seen for any span
	FirstWallclock int64       `json:",omitempty"` // first wallclock we've seen a point for this (used to query)
	LastWallClock  int64       `json:",omitempty"` // last wallclock we've seen a point for this (used to query)
	SizeSoFar      int64       `json:",omitempty"` // size of the trace so far
	Initiating     *trace.Span `json:",omitempty"` // initiating span
	CountOfSpans   int32       `json:",omitempty"` // CountOfSpans
	ToBeReleased   bool        `json:",omitempty"` // spans that have been selected to be released
}

func (b *BufferEntry) GetStartTime() int64 {
	if b.Initiating != nil && b.Initiating.Timestamp != nil {
		return *b.Initiating.Timestamp
	}
	return 0
}

//easyjson:json
type BufferEntryWithSpans struct {
	BufferEntry
	Spans [][]byte `json:",omitempty"` // spans that are already serialized
}

//easyjson:json
type BufferEntriesWithSpans []*BufferEntryWithSpans

// can't be easyjson'd due to map keys
type BufferInMemory struct {
	NumSpans       int64                `json:",omitempty"` // num spans buffered in Traces
	Entries        []BufferEntry        `json:",omitempty"` // buffer entries
	ExpiredEntries []ExpiredBufferEntry `json:",omitempty"` // expired buffer entries
	EntryMap       map[ID]int           `json:",omitempty"` // map of trace id to index of new buffer entry
	ExpiredMap     map[ID]int           `json:",omitempty"` // map of trace id to expired buffer entry
}

//easyjson:json
type BufferOnDisk struct {
	NumSpans       int64                `json:",omitempty"` // num spans buffered in Traces
	Entries        []BufferEntry        `json:",omitempty"` // buffer entries
	ExpiredEntries []ExpiredBufferEntry `json:",omitempty"` // expired buffer entries
	EntryMap       map[string]int       `json:",omitempty"` // map of trace id to index of new buffer entry
	ExpiredMap     map[string]int       `json:",omitempty"` // map of trace id to expired buffer entry
}

//easyjson:json
type SampleList []*SampleEntry

//easyjson:json
type SampleEntry struct {
	ID      *SpanIdentity `json:",omitempty"`
	Samples []int64       `json:",omitempty"`
}

//easyjson:json
type EtcdConfig struct {
	RebalanceAddress *string `json:",omitempty"`
	IngestAddress    *string `json:",omitempty"`
	ID               *string `json:",omitempty"`
}

//easyjson:json
type BufferEntryCommon struct {
	TraceID       string      `json:",omitempty"` // id of the trace
	Last          time.Time   `json:",omitempty"` // Last time we saw a span for this trace id
	LatestEndTime int64       `json:",omitempty"` // Latest end time we've seen for any span
	StartTime     int64       `json:",omitempty"` // Start time of initiating span if found
	Initiating    *trace.Span `json:",omitempty"` // initiating span
}

//easyjson:json
type ExpiredBufferEntryOld struct {
	*BufferEntryCommon
	NewSpanSeen   bool `json:",omitempty"` // we've seen a new span
	Released      bool `json:",omitempty"` // we've released this trace so send along any new spans we see
	TraceTooLarge bool `json:",omitempty"` // trace is too large so increment counters to this effect
}

//easyjson:json
type BufferEntryOld struct {
	*BufferEntryCommon
	Spans        []*trace.Span `json:",omitempty"` // buffer of spans by trace id
	SizeSoFar    int64         `json:",omitempty"` // size of the trace so far
	ToBeReleased bool          `json:",omitempty"` // spans that have been selected to be released
}

//easyjson:json
type Rebalance struct {
	Buffers        []*BufferEntryWithSpans `json:",omitempty"` // array of buffer entries with spans
	ExpiredBuffers []*ExpiredBufferEntry   `json:",omitempty"` // array of expired buffer entries
}

//easyjson:json
type RebalanceOld struct {
	Buffers        map[string]*BufferEntryOld        `json:",omitempty"` // map of trace id to buffer entry
	ExpiredBuffers map[string]*ExpiredBufferEntryOld `json:",omitempty"` // map of trace id to expired buffer entry
}
