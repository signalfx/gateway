package forwarder

import (
	"testing"
	"github.com/cep21/gohelpers/a"
	"github.com/signalfuse/signalfxproxy/core"
)

func TestBasicBufferedForwarder(t *testing.T) {
	upto := uint32(10)
	f := NewBasicBufferedForwarder(100, upto, "aname", 1)
	a.ExpectEquals(t, "aname", f.Name(), "Mismatched name")
	for i := uint32(0); i < upto + uint32(1) ; i++ {
		f.DatapointsChannel() <- nil
	}
	points, err := f.blockingDrainUpTo()
	a.ExpectEquals(t, upto, uint32(len(points)), "Expect a full set")
	a.ExpectEquals(t, nil, err, "Expect no error")
	points, err = f.blockingDrainUpTo()
	a.ExpectEquals(t, 1, len(points), "Expect one point")
	a.ExpectEquals(t, nil, err, "Expect no error")

	f = NewBasicBufferedForwarder(100, upto, "aname", 1)
	// nil should make it stop itself
	f.start(func(dp []core.Datapoint) (error) {
		return nil
	})
	f.stop()
}
