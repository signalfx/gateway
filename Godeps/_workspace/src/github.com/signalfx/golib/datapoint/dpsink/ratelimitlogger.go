package dpsink

import (
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"golang.org/x/net/context"
)

// ErrCallback is the type of callback we expect to execute on RateLimitErrorLogging non rate
// limited messages
type ErrCallback func(error)

// RateLimitErrorLogging does a logrus log of errors forwarding points in a rate limited manner
type RateLimitErrorLogging struct {
	lastLogTimeNs int64
	LogThrottle   time.Duration
	Callback      ErrCallback
}

// AddDatapoints forwards points and will log any errors forwarding, but only one per LogThrottle
// duration
func (e *RateLimitErrorLogging) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint, next Sink) error {
	err := next.AddDatapoints(ctx, points)
	if err != nil {
		now := time.Now()
		lastLogTimeNs := atomic.LoadInt64(&e.lastLogTimeNs)
		sinceLastLogNs := now.UnixNano() - lastLogTimeNs
		if sinceLastLogNs > e.LogThrottle.Nanoseconds() {
			nowUnixNs := now.UnixNano()
			if atomic.CompareAndSwapInt64(&e.lastLogTimeNs, lastLogTimeNs, nowUnixNs) {
				e.Callback(err)
			}
		}
	}
	return err
}

// AddEvents forwards points and will log any errors forwarding, but only one per LogThrottle
// duration
func (e *RateLimitErrorLogging) AddEvents(ctx context.Context, points []*event.Event, next Sink) error {
	err := next.AddEvents(ctx, points)
	if err != nil {
		now := time.Now()
		lastLogTimeNs := atomic.LoadInt64(&e.lastLogTimeNs)
		sinceLastLogNs := now.UnixNano() - lastLogTimeNs
		if sinceLastLogNs > e.LogThrottle.Nanoseconds() {
			nowUnixNs := now.UnixNano()
			if atomic.CompareAndSwapInt64(&e.lastLogTimeNs, lastLogTimeNs, nowUnixNs) {
				e.Callback(err)
			}
		}
	}
	return err
}

// LogCallback returns a callback that logs an error to logger with msg at Warn
func LogCallback(msg string, l *log.Logger) ErrCallback {
	return func(err error) {
		l.WithField("err", err.Error()).Warn(msg)
	}
}
