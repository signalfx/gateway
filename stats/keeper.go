package stats

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/signalfx/metricproxy/datapoint"
)

// A Keeper contains datapoints that describe its state and can be reported upstream
type Keeper interface {
	Stats() []datapoint.Datapoint
}

// A ClosableKeeper can report datapoints and close itself.  Usually, services open ports and
// will eventually need to be closed.  Honestly, this type may belong in another package (?)
type ClosableKeeper interface {
	Keeper
	Close()
}

// DimensionKeeper works like a stat keeper but requires input dimensions to any stats
// it creates
type DimensionKeeper interface {
	Stats(dimensions map[string]string) []datapoint.Datapoint
}

// KeeperWrap pretends to be a Keeper by wrapping DimensionKeeper with dimensions
type KeeperWrap struct {
	Base       []DimensionKeeper
	Dimensions map[string]string
}

// Stats returns any stats from this wrappers stat keepers
func (k *KeeperWrap) Stats() []datapoint.Datapoint {
	r := []datapoint.Datapoint{}
	for _, s := range k.Base {
		r = append(r, s.Stats(k.Dimensions)...)
	}
	return r
}

// CombineStats from multiple keepers in the order given as parameters.
func CombineStats(keepers []Keeper) []datapoint.Datapoint {
	ret := []datapoint.Datapoint{}
	for _, r := range keepers {
		ret = append(ret, r.Stats()...)
	}
	return ret
}

// StatKeepingStreamer both keeps stats and can stream datapoints
type StatKeepingStreamer interface {
	datapoint.Streamer
	Keeper
	Name() string
}

// DrainingThread attaches to the metricproxy to periodically send proxy statistics to
// listeners
type DrainingThread interface {
	SendStats()
	Stats() []datapoint.Datapoint
	Start()
}

type statDrainingThreadImpl struct {
	delay       time.Duration
	sendTo      []datapoint.Streamer
	listenFrom  []Keeper
	stopChannel <-chan bool
}

// NewDrainingThread returns a new DrainingThread.  The user must explicitly call
// "go item.Start()" on the returned item.
func NewDrainingThread(delay time.Duration, sendTo []datapoint.Streamer, listenFrom []Keeper, stopChannel <-chan bool) DrainingThread {
	return &statDrainingThreadImpl{
		delay:       delay,
		sendTo:      sendTo,
		listenFrom:  listenFrom,
		stopChannel: stopChannel,
	}
}

func (thread *statDrainingThreadImpl) Stats() []datapoint.Datapoint {
	points := []datapoint.Datapoint{}
	for _, listenFrom := range thread.listenFrom {
		log.WithField("listenFrom", listenFrom).Debug("Loading stats")
		stats := listenFrom.Stats()
		log.WithField("stats", stats).Debug("Stats loaded")
		points = append(points, listenFrom.Stats()...)
	}
	return points
}

func (thread *statDrainingThreadImpl) SendStats() {
	points := thread.Stats()
	for _, sendTo := range thread.sendTo {
		for _, dp := range points {
			sendTo.Channel() <- dp
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
