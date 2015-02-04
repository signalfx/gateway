package core

import (
	"time"

	log "github.com/Sirupsen/logrus"
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

// DimensionStatKeeper works like a stat keeper but requires input dimensions to any stats
// it creates
type DimensionStatKeeper interface {
	GetStats(dimensions map[string]string) []Datapoint
}

// StatKeeperWrap pretends to be a StatKeeper by wrapping DimensionStatKeeper with dimensions
type StatKeeperWrap struct {
	Base       []DimensionStatKeeper
	Dimensions map[string]string
}

// GetStats returns any stats from this wrappers stat keepers
func (k *StatKeeperWrap) GetStats() []Datapoint {
	r := []Datapoint{}
	for _, s := range k.Base {
		r = append(r, s.GetStats(k.Dimensions)...)
	}
	return r
}

// CombineStats from multiple keepers in the order given as parameters.
func CombineStats(keepers []StatKeeper) []Datapoint {
	ret := []Datapoint{}
	for _, r := range keepers {
		ret = append(ret, r.GetStats()...)
	}
	return ret
}

// StatKeepingStreamingAPI both keeps stats and can stream datapoints
type StatKeepingStreamingAPI interface {
	DatapointStreamingAPI
	StatKeeper
}

// StatDrainingThread attaches to the signalfxproxy to periodically send proxy statistics to
// listeners
type StatDrainingThread interface {
	SendStats()
	GetStats() []Datapoint
	Start()
}

type statDrainingThreadImpl struct {
	delay       time.Duration
	sendTo      []DatapointStreamingAPI
	listenFrom  []StatKeeper
	stopChannel <-chan bool
}

// NewStatDrainingThread returns a new StatDrainingThread.  The user must explicitly call
// "go item.Start()" on the returned item.
func NewStatDrainingThread(delay time.Duration, sendTo []DatapointStreamingAPI, listenFrom []StatKeeper, stopChannel <-chan bool) StatDrainingThread {
	return &statDrainingThreadImpl{
		delay:       delay,
		sendTo:      sendTo,
		listenFrom:  listenFrom,
		stopChannel: stopChannel,
	}
}

func (thread *statDrainingThreadImpl) GetStats() []Datapoint {
	points := []Datapoint{}
	for _, listenFrom := range thread.listenFrom {
		log.WithField("listenFrom", listenFrom).Debug("Loading stats")
		stats := listenFrom.GetStats()
		log.WithField("stats", stats).Debug("Stats loaded")
		points = append(points, listenFrom.GetStats()...)
	}
	return points
}

func (thread *statDrainingThreadImpl) SendStats() {
	points := thread.GetStats()
	for _, sendTo := range thread.sendTo {
		for _, dp := range points {
			sendTo.DatapointsChannel() <- dp
		}
	}
}

func (thread *statDrainingThreadImpl) Start() {
	log.WithField("listenFrom", thread.listenFrom).Info("Draining stats")
	for {
		select {
		case _ = <-thread.stopChannel:
			log.Debug("Request to stop stat thread")
			return
		case _ = <-time.After(thread.delay):
			log.Debug("Stat thread waking up")
		}
		thread.SendStats()
	}
}
