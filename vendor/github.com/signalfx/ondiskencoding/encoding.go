package encoding

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/signalfx/golib/trace"
)

const trueStr = "true"

// low, high
type ID [2]uint64

var errInvalid = errors.New("ID is not a 16 or 32 byte hex string")

func GetID(s string) (ID, error) {
	var err error
	var low, high uint64
	switch len(s) {
	case 16:
		low, err = strconv.ParseUint(s, 16, 64)
	case 32:
		high, err = strconv.ParseUint(s[:16], 16, 64)
		if err == nil {
			low, err = strconv.ParseUint(s[16:], 16, 64)
		}
	default:
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

// NewExtendedSpanIdentity returns a SpanIdentity with additional dimensions applied
func NewExtendedSpanIdentity(baseID *SpanIdentity, additionalDims map[string]string) *SpanIdentity {
	si := &SpanIdentity{
		Service:     baseID.Service,
		Operation:   baseID.Operation,
		HttpMethod:  baseID.HttpMethod,
		Kind:        baseID.Kind,
		Error:       baseID.Error,
		ServiceMesh: baseID.ServiceMesh,
	}
	if bb, _ := json.Marshal(additionalDims); bb != nil {
		si.AdditionalDimensions = string(bb)
	}
	return si
}

// SpanIdentity is a tuple of Service and Operation
//easyjson:json
type SpanIdentity struct {
	Service              string `json:",omitempty"`
	Operation            string `json:",omitempty"`
	HttpMethod           string `json:",omitempty"`
	Kind                 string `json:",omitempty"`
	AdditionalDimensions string `json:",omitempty"`
	Error                bool   `json:",omitempty"`
	ServiceMesh          bool   `json:",omitempty"`
}

func (v *SpanIdentity) String() string {
	s := ""
	if v.HttpMethod != "" {
		s += ", HttpMethod:" + v.HttpMethod
	}
	if v.Kind != "" {
		s += ", Kind:" + v.Kind
	}
	if v.ServiceMesh {
		s += ", ServiceMesh:true" + v.Kind
	}
	if v.AdditionalDimensions != "" {
		s += ", AdditionalDimensions:" + v.AdditionalDimensions
	}
	return fmt.Sprintf("Identity[Service:%s, Operation:%s, Error:%t%s]", v.Service, v.Operation, v.Error, s)
}

func (v *SpanIdentity) Dims() map[string]string {
	var m = map[string]string{}

	// unmarshall additional dimensions to the map first.  that way if there'v a dim called "service" or "operation"
	// it will be overwritten by the proper service or operation of the span identity.
	if v.AdditionalDimensions != "" {
		m["sf_dimensionalized"] = trueStr
		_ = json.Unmarshal([]byte(v.AdditionalDimensions), &m)
	}

	// ensure that the dims for span identity fields are set over top of the additional dims that were unmarshaled
	m["service"] = v.Service
	m["operation"] = v.Operation

	if v.Error {
		m["error"] = trueStr
	} else {
		m["error"] = "false"
	}
	if v.HttpMethod != "" {
		m["httpMethod"] = v.HttpMethod
	}
	if v.Kind != "" {
		m["kind"] = v.Kind
	}
	if v.ServiceMesh {
		m["sf_serviceMesh"] = trueStr
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
	Entries map[SpanIdentity]HistoOnDiskEntry `json:",omitempty"`
}

//easyjson:json
type Histo struct {
	Error *HistoOnDisk `json:",omitempty"`
	Span  *HistoOnDisk `json:",omitempty"`
	Trace *HistoOnDisk `json:",omitempty"`
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

func (v *ExpiredBufferEntry) HasInitiating() bool {
	return v.InitiatingIdentity.Service != "" && v.InitiatingIdentity.Operation != ""
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

func (v *BufferEntry) GetStartTime() int64 {
	if v.Initiating != nil && v.Initiating.Timestamp != nil {
		return *v.Initiating.Timestamp
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
	RebalanceAddress string `json:",omitempty"`
	IngestAddress    string `json:",omitempty"`
	ID               string `json:",omitempty"`
	Weight           int32  `json:",omitempty"`
	Version          int32  `json:",omitempty"`
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
