package stats

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"golang.org/x/net/context"
)

// A Keeper contains datapoints that describe its state and can be reported upstream
type Keeper interface {
	Stats() []*datapoint.Datapoint
}

type combinedStats []Keeper

// CombineStats from multiple keepers in the order given as parameters.
func (c combinedStats) Stats() []*datapoint.Datapoint {
	ret := []*datapoint.Datapoint{}
	for _, r := range c {
		ret = append(ret, r.Stats()...)
	}
	return ret
}

// Combine multiple keepers into a single keeper
func Combine(keepers ...Keeper) Keeper {
	return combinedStats(keepers)
}

// A DimensionKeeper contains datapoints that describe its state and can be reported upstream
type DimensionKeeper interface {
	Stats(dimensions map[string]string) []*datapoint.Datapoint
}

type dimKeeper struct {
	k    DimensionKeeper
	dims map[string]string
}

// ToKeeper creates a keeper that pulls stats from k with dims
func ToKeeper(k DimensionKeeper, dims map[string]string) Keeper {
	return &dimKeeper{
		k:    k,
		dims: dims,
	}
}

// ToKeeperMany creates a single keeper that pulls stats from each of keeps with dims
func ToKeeperMany(dims map[string]string, keeps ...DimensionKeeper) Keeper {
	ret := make([]Keeper, 0, len(keeps))
	for _, k := range keeps {
		ret = append(ret, ToKeeper(k, dims))
	}
	return Combine(ret...)
}

func (d *dimKeeper) Stats() []*datapoint.Datapoint {
	return d.k.Stats(d.dims)
}

// StatDrainingThread will sleep delay between draining stats from each keeper and sending the full
// stats to each sendTo sink
type StatDrainingThread struct {
	delay      time.Duration
	sendTo     []dpsink.Sink
	listenFrom []Keeper
	ctx        context.Context
}

// NewDrainingThread returns a new DrainingThread to collect stats and send them to sinks
func NewDrainingThread(delay time.Duration, sendTo []dpsink.Sink, listenFrom []Keeper, ctx context.Context) *StatDrainingThread {
	ret := &StatDrainingThread{
		delay:      delay,
		sendTo:     sendTo,
		listenFrom: listenFrom,
		ctx:        ctx,
	}
	go ret.start()
	return ret
}

// Stats returns all the stast this draining thread will report
func (thread *StatDrainingThread) Stats() []*datapoint.Datapoint {
	points := []*datapoint.Datapoint{}
	for _, listenFrom := range thread.listenFrom {
		log.WithField("listenFrom", listenFrom).Debug("Loading stats")
		stats := listenFrom.Stats()
		log.WithField("stats", stats).Debug("Stats loaded")
		points = append(points, listenFrom.Stats()...)
	}
	return points
}

func (thread *StatDrainingThread) start() error {
	log.WithField("listenFrom", thread.listenFrom).Info("Draining stats")
	for {
		select {
		case <-thread.ctx.Done():
			log.Debug("Request to stop stat thread")
			return thread.ctx.Err()
		case <-time.After(thread.delay):
			points := thread.Stats()
			for _, sendTo := range thread.sendTo {
				// Note: Errors ignored
				sendTo.AddDatapoints(thread.ctx, points)
			}
		}
	}
}
