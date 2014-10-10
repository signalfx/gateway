package core

import (
	"github.com/golang/glog"
	"time"
)

// DatapointStreamingAPI is the interface servers we send data to
// must implement
type DatapointStreamingAPI interface {
	DatapointsChannel() chan<- Datapoint
	Name() string
}

// A StatKeeper contains datapoints that describe its state and can be reported upstream
type StatKeeper interface {
	GetStats() []Datapoint
}

// StatKeepingStreamingAPI both keeps stats and can stream datapoints
type StatKeepingStreamingAPI interface {
	DatapointStreamingAPI
	StatKeeper
}

// DrainStatsThread starts the stats listening thread that sleeps delay amount between gathering
// and sending stats
func DrainStatsThread(delay time.Duration, sendTo []DatapointStreamingAPI, listenFrom []StatKeeper, stopChannel <- chan bool) {
	glog.Infof("Draining stats from %s", listenFrom)
	for {
		select {
		case _ = <- stopChannel:
			glog.V(1).Infof("Request to stop stat thread")
			return
		case _ = <- time.After(delay):
			glog.V(3).Infof("Stat thread waking up")
		}

		points := []Datapoint{}
		for _, listenFrom := range listenFrom {
			glog.V(1).Infof("Loading from  %s", listenFrom)
			stats := listenFrom.GetStats()
			glog.V(1).Infof("Stats are %s", stats)
			points = append(points, listenFrom.GetStats()...)
		}
		for _, sendTo := range sendTo {
			for _, dp := range points {
				sendTo.DatapointsChannel() <- dp
			}
		}
	}
}
