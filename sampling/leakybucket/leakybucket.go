package leakybucket

import (
	"fmt"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/sfxinternalgo/lib/sfxthrift/thrift/datamodel"
	"github.com/signalfx/sfxinternalgo/lib/sfxthrift/thrift/id"
	"math"
	"sync/atomic"
	"time"
)

// LeakyBucket is a bucket with a leak
// Imagine a bucket, which fills as you perform actions. You are not allowed to perform an action
// that would overflow the bucket. Now imagine it has a leak, at a specific rate, so over time
// capacity returns to the bucket allowing you add more to the bucket.
// Now imagine there are several buckets, where a service fills every other bucket how much it is
// filled. In this case we totally break the metaphor and allow a bucket to be over filled so that
// actions aren't allowed for free, but you pay for them in the long run.
// To avoid locks and such it's single threaded.
type LeakyBucket struct {
	timeKeeper             timekeeper.TimeKeeper
	leakDuration           int64
	actionsThisDuration    int64
	actionLimitPerDuration int64
	timeDurationInNs       int64
	lastActionTimeInNs     int64
}

type throttleError struct {
	actionLimitPerDuration int64
	actionsSoFar           int64
	actions                int64
}

func (e *throttleError) Error() string {
	return fmt.Sprintf("leaky bucket throttle actionsSoFar:%d limit:%d current:%d", e.actionsSoFar, e.actionLimitPerDuration, e.actions)
}

func newError(actionsLimitPerDuration, actionsSoFar, actions int64) error {
	return &throttleError{
		actionLimitPerDuration: actionsLimitPerDuration,
		actionsSoFar:           actionsSoFar,
		actions:                actions,
	}
}

func (l *LeakyBucket) leak(currentTime time.Time) int64 {
	for {
		lastActionTime := atomic.LoadInt64(&l.lastActionTimeInNs)
		actionsSoFar := atomic.LoadInt64(&l.actionsThisDuration)
		diff := currentTime.UnixNano() - lastActionTime
		duration := atomic.LoadInt64(&l.leakDuration)
		drips := diff / duration
		actionsSoFar -= drips
		if actionsSoFar < 0 {
			actionsSoFar = 0
		} else {
			diff = drips * duration
		}
		if atomic.CompareAndSwapInt64(&l.lastActionTimeInNs, lastActionTime, lastActionTime+diff) {
			// if the times haven't changed, this should be guaranteed to be correct since we set them in this order
			atomic.StoreInt64(&l.actionsThisDuration, actionsSoFar)
			return actionsSoFar
		}
	}
}

func (l *LeakyBucket) innerCheck(currentTime time.Time, actions int64) (err error) {
	actionsSoFar := l.leak(currentTime)
	actionLimit := atomic.LoadInt64(&l.actionLimitPerDuration)
	if actions > actionLimit || actionsSoFar+actions > actionLimit {
		err = newError(actionLimit, actionsSoFar, actions)
	}
	return
}

// Deduct deducts the amount from what the bucket can use
func (l *LeakyBucket) Deduct(actions int64) {
	atomic.AddInt64(&l.actionsThisDuration, actions)
}

// CheckAction checks if able to perform local action
func (l *LeakyBucket) CheckAction(actions int64) error {
	return l.innerCheck(l.timeKeeper.Now(), actions)
}

// Fill from remote service
// since these came from possibly seconds ago, reduce them if possible
func (l *LeakyBucket) Fill(actions int64) error {
	ret := l.innerCheck(l.timeKeeper.Now(), actions)
	l.Deduct(actions)
	return ret
}

// CurrentActionLimit returns just that, useful to see if throttles have changed
func (l *LeakyBucket) CurrentActionLimit() int64 {
	return atomic.LoadInt64(&l.actionLimitPerDuration)
}

// Reset resets the actions
func (l *LeakyBucket) Reset() {
	for {
		lastActionTime := atomic.LoadInt64(&l.lastActionTimeInNs)
		now := l.timeKeeper.Now().UnixNano()
		if atomic.CompareAndSwapInt64(&l.lastActionTimeInNs, lastActionTime, now) {
			// if the times haven't changed, this should be guaranteed to be correct since we set them in this order
			atomic.StoreInt64(&l.actionsThisDuration, 0)
			return
		}
	}
}

// ChangeActionLimit in a synchronized, yet non-locking way
func (l *LeakyBucket) ChangeActionLimit(newLimit int64) {
	for {
		var leakDuration int64
		if newLimit == 0 {
			leakDuration = math.MaxInt64
		} else {
			leakDuration = l.timeDurationInNs / newLimit
		}
		if leakDuration == 0 {
			leakDuration = 1
		}
		lastActionTime := atomic.LoadInt64(&l.lastActionTimeInNs)
		now := l.timeKeeper.Now().UnixNano()
		if atomic.CompareAndSwapInt64(&l.lastActionTimeInNs, lastActionTime, now) {
			// if the times haven't changed, this should be guaranteed to be correct since we set them in this order
			atomic.StoreInt64(&l.actionLimitPerDuration, newLimit)
			atomic.StoreInt64(&l.leakDuration, leakDuration)
			// don't reset the throttle, keep where we were at, can use sfc reset to reset if we want to
			return
		}
	}
}

// ActionsThisDuration leaks required amount by calling CheckAction then returns the actionsThisDuration like it says
func (l *LeakyBucket) ActionsThisDuration() int64 {
	l.CheckAction(0)
	return atomic.LoadInt64(&l.actionsThisDuration)
}

// GenerateThrottleResponse returns a template for a throttling response that would need the owner filled out
func (l *LeakyBucket) GenerateThrottleResponse() *datamodel.ThrottlingResponse {
	l.leak(l.timeKeeper.Now())
	resp := datamodel.NewThrottlingResponse()
	resp.Owner = datamodel.NewTokenOwner()
	resp.Owner.OrgId = id.NewID()
	resp.Owner.NamedTokenId = id.NewID()
	resp.Owner.TeamId = id.NewID()
	resp.Spread = -1
	resp.LimitPerTimeUnit = atomic.LoadInt64(&l.actionLimitPerDuration)
	resp.TimeUnitInNs = l.timeDurationInNs
	resp.CurrentUsed = atomic.LoadInt64(&l.actionsThisDuration)
	resp.Spread = -1
	// overloading for how long till empty
	resp.TimeLeftInPeriod = resp.CurrentUsed * atomic.LoadInt64(&l.leakDuration)
	return resp
}

func (l *LeakyBucket) String() string {
	return fmt.Sprintf("ActionLimitPerDuration: %d TimeUnitInNs: %d CurrentlyUsed: %d", atomic.LoadInt64(&l.actionLimitPerDuration), l.timeDurationInNs, atomic.LoadInt64(&l.actionsThisDuration))
}

// New returns a new leaky bucket
func New(timeKeeper timekeeper.TimeKeeper, actionLimitPerDuration int64, timeDuration time.Duration) *LeakyBucket {
	l := &LeakyBucket{
		timeKeeper:             timeKeeper,
		actionLimitPerDuration: actionLimitPerDuration,
		timeDurationInNs:       timeDuration.Nanoseconds(),
		lastActionTimeInNs:     timeKeeper.Now().UnixNano(),
	}
	l.ChangeActionLimit(actionLimitPerDuration)
	return l
}
