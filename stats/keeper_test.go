package stats

import (
	"testing"

	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

type statKeeper struct {
}

func (m *statKeeper) Stats() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{nil}
}

type dimKeeperTest struct {
}

func (m *dimKeeperTest) Stats(dims map[string]string) []*datapoint.Datapoint {
	return []*datapoint.Datapoint{nil}
}

func TestStatDrainingThreadSend(t *testing.T) {
	testSink := dptest.NewBasicSink()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	drainer := NewDrainingThread(time.Millisecond, []dpsink.Sink{testSink}, []Keeper{&statKeeper{}}, ctx)
	assert.Equal(t, 1, len(drainer.Stats()))
	<-testSink.PointsChan
}

func TestStatDrainingThreadCancel(t *testing.T) {
	testSink := dptest.NewBasicSink()
	ctx, cancel := context.WithCancel(context.Background())
	drainer := NewDrainingThread(time.Hour, []dpsink.Sink{testSink}, []Keeper{&statKeeper{}}, ctx)
	cancel()
	assert.Equal(t, ctx.Err(), drainer.start())
}

func TestCombined(t *testing.T) {
	k1 := &statKeeper{}
	k2 := &statKeeper{}
	c := Combine(k1, k2)
	assert.Equal(t, 2, len(c.Stats()))
}

func TestToKeeperMany(t *testing.T) {
	k1 := &dimKeeperTest{}
	k2 := &dimKeeperTest{}
	c := ToKeeperMany(map[string]string{}, k1, k2)
	assert.Equal(t, 2, len(c.Stats()))
}
