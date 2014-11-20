package listener

import (
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
)

// A DatapointListener is an object that listens for input datapoints
type DatapointListener interface {
	core.StatKeeper
	Close()
}

// A Loader loads a DatapointListener from a configuration definition
type Loader func(core.DatapointStreamingAPI, *config.ListenFrom) (DatapointListener, error)

// AllListenerLoaders is a map of all loaders from config, for each listener we support
var AllListenerLoaders = map[string]Loader{
	"signalfx": SignalFxListenerLoader,
	"carbon":   CarbonListenerLoader,
	"collectd": CollectdListenerLoader,
}
