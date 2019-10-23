package hubclient

import (
	"context"
	"github.com/mailru/easyjson"
)

// Client interface for interacting with the gateway hub
type Client interface {
	// Distributors returns a list of cluster servers on demand
	Distributors(context.Context) ([]*Server, error)
	// Servers returns a list of cluster servers on demand
	Servers(context.Context) ([]*Server, error)
	// WatchServers returns a channel that will receive serverUpdates updates and a cancel function to end the watch
	WatchServers() (<-chan []*Server, context.CancelFunc)
	// WatchDistributors returns a channel to watch for distributor updates and a cancel function to end the watch
	WatchDistributors() (<-chan []*Server, context.CancelFunc)
	// WatchConfig returns a channel that will receive config updates and a cancel function to end the watch
	WatchConfig() (<-chan *Config, context.CancelFunc)
}

// ServerResponse represents a SmartGateway Server as returned by the hub api
// easyjson:json
type ServerResponse struct {
	// Name is the name of the server
	Name string `json:"name"`
	// Version is the smart gateway version of the server
	Version string `json:"version"`
	// Payload is the payload for the server
	Payload []byte `json:"payload"`
	// LastHeartbeat is the timestamp of the last heartbeat
	// TODO this should be a string that we parse out a timestamp from
	LastHeartbeat int `json:"lastHeartbeat"`
}

// String is the ToString method for ServerResponse
func (v ServerResponse) String() string {
	bts, _ := easyjson.Marshal(v)
	return string(bts)
}

// ToServer returns a Server struct copying the fields of the ServerResponse with the payload unmarshalled
func (v *ServerResponse) ToServer() Server {
	s := Server{
		ServerResponse: ServerResponse{
			Name:          v.Name,
			Version:       v.Version,
			LastHeartbeat: v.LastHeartbeat,
		},
		// TODO parse out a date from lastHeartBeat
		Payload: ServerPayload(make(map[string][]byte)),
	}

	// Attempt to unmarshal the payload and ignore errors. Ignoring errors here is ok,
	// because things reading from the Server.ServerPayload should handle the situation where
	// their key doesn't exist.
	easyjson.Unmarshal(v.Payload, &s.Payload)

	return s
}

// ServerPayload is a map of payload byte arrays
// easyjson:json
type ServerPayload map[string][]byte

// Server represents a SmartGateway Server with its payload decrypted and unmarshalled
// easyjson:json
type Server struct {
	ServerResponse
	// Payload is a map of payloads ([]byte) by forwarder/listener name
	Payload ServerPayload `json:"payload"`
}

// String is the ToString method for Server
func (v Server) String() string {
	bts, _ := easyjson.Marshal(v)
	return string(bts)
}

// Cluster represents a Smart Gateway Cluster
// easyjson:json
type Cluster struct {
	// Name is the name of the cluster
	Name string `json:"name"`
	// Servers are the servers in the cluster
	Servers []*ServerResponse `json:"servers"`
	// Distributors are the distributors in the cluster
	Distributors []*ServerResponse `json:"distributors"`
}

// String is the ToString method for Cluster
func (v Cluster) String() string {
	bts, _ := easyjson.Marshal(v)
	return string(bts)
}

// Budget is budget information returned by the hub
// easyjson:json
type Budget struct {
	// ErrorBudget is the budget for errors returned by the hub
	ErrorBudget float64 `json:"eb"`
	// SpanBudget is the budget for spans returned by the hub
	SpanBudget float64 `json:"sb"`
	// TraceBudget is the budget for traces returned by the hub
	TraceBudget float64 `json:"tb"`
}

// String is the ToString method for Budget
func (v Budget) String() string {
	bts, _ := easyjson.Marshal(v)
	return string(bts)
}

// Config is configuration returned by the hub
// easyjson:json
type Config struct {
	// Budget is the budget information returned by the hub
	Budget *Budget `json:"budget,omitempty"`
	// Heartbeat is the heartbeat frequency returned by the hub
	Heartbeat int64 `json:"hb"`
	// SpanSize is the max span size configuration returned by the hub
	SpanSize float64 `json:"ss"`
	// TraceSize is the max trace size configuration returned by the hub
	TraceSize float64 `json:"ts"`
}

// String is the ToString method for Config
func (v Config) String() string {
	bts, _ := easyjson.Marshal(v)
	return string(bts)
}

// Registration is the payload sent to the hub for registration
// easyjson:json
type Registration struct {
	// Cluster is the name of the cluster
	Cluster string `json:"cluster"`
	// Name is the name of the server
	Name string `json:"name"`
	// Version is the smart gateway version of the server
	Version string `json:"version"`
	// Payload is the payload to send to the gateway hub
	Payload []byte `json:"payload"`
	// Distributor indicates if the server is a distributor or not
	Distributor bool `json:"distributor"`
}

// String is the ToString method for Registration
func (v Registration) String() string {
	bts, _ := easyjson.Marshal(v)
	return string(bts)
}

// RegistrationResponse is the response from the hub's registration endpoint
// easyjson:json
type RegistrationResponse struct {
	// Lease is the instance lease for the server in the cluster
	Lease string `json:"lease"`
	// Config is the configuration struct returned by the hub
	Config *Config `json:"config,omitempty"`
	// Cluster is the state of the cluster known by the hub
	Cluster *Cluster `json:"cluster,omitempty"`
}

// String is the ToString method for RegistrationResponse
func (v RegistrationResponse) String() string {
	bts, _ := easyjson.Marshal(v)
	return string(bts)
}

// HeartbeatResponse is the response returned when heartbeating
// easyjson:json
type HeartbeatResponse struct {
	// Config is the configuration struct returned by the hub
	Config *Config `json:"config,omitempty"`
	// Cluster is the state of the cluster known by the hub
	Cluster *Cluster `json:"cluster,omitempty"`
}

func (v HeartbeatResponse) String() string {
	bts, _ := easyjson.Marshal(v)
	return string(bts)
}
