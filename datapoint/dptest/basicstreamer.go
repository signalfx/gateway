package dptest

import "github.com/signalfx/metricproxy/datapoint"

// BasicStreamer is the simplest implementation of a datapoint.streamer
type BasicStreamer struct {
	Chan chan datapoint.Datapoint
}

// Channel returns the datapoint chan for this streamer
func (api *BasicStreamer) Channel() chan<- datapoint.Datapoint {
	return api.Chan
}

// Name returns empty string
func (api *BasicStreamer) Name() string {
	return ""
}
