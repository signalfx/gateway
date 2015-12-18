package dpsink

import (
	"errors"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/event"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestRateLimitErrorLogging(t *testing.T) {
	expectedErr := errors.New("nope")
	end := dptest.NewBasicSink()
	end.RetError(expectedErr)
	ctx := context.Background()
	var lastErr error
	count := 0
	l := RateLimitErrorLogging{
		Callback: func(err error) {
			lastErr = err
			count++
		},
		LogThrottle: time.Second,
	}
	for i := 0; i < 1000; i++ {
		assert.Equal(t, expectedErr, l.AddDatapoints(ctx, []*datapoint.Datapoint{dptest.DP()}, end))
	}
	assert.Equal(t, expectedErr, lastErr)
	assert.Equal(t, 1, count)

}

func TestRateLimitErrorLoggingEvent(t *testing.T) {
	expectedErr := errors.New("nope")
	end := dptest.NewBasicSink()
	end.RetError(expectedErr)
	ctx := context.Background()
	var lastErr error
	count := 0
	l := RateLimitErrorLogging{
		Callback: func(err error) {
			lastErr = err
			count++
		},
		LogThrottle: time.Second,
	}
	for i := 0; i < 1000; i++ {
		assert.Equal(t, expectedErr, l.AddEvents(ctx, []*event.Event{dptest.E()}, end))
	}
	assert.Equal(t, expectedErr, lastErr)
	assert.Equal(t, 1, count)

}

func TestLogCallback(t *testing.T) {
	LogCallback("", logrus.StandardLogger())(errors.New("hi"))
}
