package encoding

import "time"

// SpanIdentity is a tuple of Service and Operation
//easyjson:json
type SpanIdentity struct {
	Service   string
	Operation string
}

func (k *SpanIdentity) Dims() map[string]string {
	return map[string]string{
		"service":   k.Service,
		"operation": k.Operation,
	}
}

//easyjson:json
type OnDisk struct {
	Digests              map[SpanIdentity][]int64   `json:",omitempty"`
	Last                 map[SpanIdentity]time.Time `json:",omitempty"`
	MetricsReservoirSize int                        `json:",omitempty"`
	MetricsAlphaFactor   float64                    `json:",omitempty"`
}
