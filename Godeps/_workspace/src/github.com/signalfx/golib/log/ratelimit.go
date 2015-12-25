package log

import (
	"github.com/signalfx/golib/eventcounter"
	"time"
)

// A RateLimitedLogger will disable itself after a Limit number of log messages in a period.
// This can be useful if you want to prevent your logger from killing the thing it is logging to
// such as disk or network.
type RateLimitedLogger struct {
	EventCounter eventcounter.EventCounter
	Limit        int64
	Logger       Logger
	LimitLogger  Logger
	Now          func() time.Time
}

func (r *RateLimitedLogger) now() time.Time {
	if r.Now == nil {
		return time.Now()
	}
	return r.Now()
}

// Log kvs to the wrapped Logger if the limit hasn't been reached
func (r *RateLimitedLogger) Log(kvs ...interface{}) {
	now := r.now()
	if r.EventCounter.Event(now) > r.Limit {
		if r.LimitLogger != nil {
			r.LimitLogger.Log(kvs...)
		}
		return
	}
	r.Logger.Log(kvs...)
}

// Disabled returns true if this logger is over its limit or if the wrapped logger is
// disabled.
func (r *RateLimitedLogger) Disabled() bool {
	now := r.now()
	return r.EventCounter.Events(now, 0) > r.Limit || IsDisabled(r.Logger)
}
