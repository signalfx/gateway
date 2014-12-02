package core

import (
	log "github.com/Sirupsen/logrus"
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
func DrainStatsThread(delay time.Duration, sendTo []DatapointStreamingAPI, listenFrom []StatKeeper, stopChannel <-chan bool) {
	log.WithField("listenFrom", listenFrom).Info("Draining stats")
	for {
		select {
		case _ = <-stopChannel:
			log.Debug("Request to stop stat thread")
			return
		case _ = <-time.After(delay):
			log.Debug("Stat thread waking up")
		}

		points := []Datapoint{}
		for _, listenFrom := range listenFrom {
			log.WithField("listenFrom", listenFrom).Debug("Loading stats")
			stats := listenFrom.GetStats()
			log.WithField("stats", stats).Debug("Stats loaded")
			points = append(points, listenFrom.GetStats()...)
		}
		for _, sendTo := range sendTo {
			for _, dp := range points {
				sendTo.DatapointsChannel() <- dp
			}
		}
	}
}
