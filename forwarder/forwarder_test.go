package forwarder

import (
	"testing"
	"github.com/cep21/gohelpers/a"
	"github.com/signalfuse/signalfxproxy/core"
	"time"
	"errors"
	"github.com/golang/glog"
)

func TestBasicBufferedForwarder(t *testing.T) {
	upto := uint32(10)
	f := NewBasicBufferedForwarder(100, upto, "aname", 1)
	a.ExpectEquals(t, "aname", f.Name(), "Mismatched name")
	for i := uint32(0); i < upto + uint32(1) ; i++ {
		f.DatapointsChannel() <- nil
	}
	points  := f.blockingDrainUpTo()
	a.ExpectEquals(t, upto, uint32(len(points)), "Expect a full set")
	points = f.blockingDrainUpTo()
	a.ExpectEquals(t, 1, len(points), "Expect one point")
}

func TestStopForwarder(t *testing.T) {
	f := NewBasicBufferedForwarder(100, uint32(10), "aname", 1)
	f.DatapointsChannel() <- nil
	f.DatapointsChannel() <- nil
	glog.Info("Hello")
	seenPoints := 0
	// nil should make it stop itself
	f.start(func(dp []core.Datapoint) (error) {
		seenPoints += len(dp)
		return errors.New("unable to process")
	})
	time.Sleep(time.Millisecond * 5)
	f.stop()
	a.ExpectEquals(t, 2, seenPoints, "Expect two points")
	a.ExpectNotEquals(t, nil, f.start(nil), "Shouldn't be able to start twice")
	f.blockingDrainStopChan <- true
	f.stop() // Should not block if chan has an item
}
