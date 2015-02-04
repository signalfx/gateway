package listener

import (
	"net"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
)

// A DatapointListener is an object that listens for input datapoints
type DatapointListener interface {
	core.StatKeeper
	Close()
}

// A NetworkListener is a listener that looks for data on a network address.  It is sometimes
// useful in testing to get this address so you can talk to it directly.
type NetworkListener interface {
	GetAddr() net.Addr
}

// A Loader loads a DatapointListener from a configuration definition
type Loader func(core.DatapointStreamingAPI, *config.ListenFrom) (DatapointListener, error)

// AllListenerLoaders is a map of all loaders from config, for each listener we support
var AllListenerLoaders = map[string]Loader{
	"signalfx": SignalFxListenerLoader,
	"carbon":   CarbonListenerLoader,
	"collectd": CollectdListenerLoader,
}
