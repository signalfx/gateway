package stats

import (
	"testing"

	"time"

	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/datapoint/dpsink"
	"github.com/signalfx/metricproxy/datapoint/dptest"
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

func TestStatDrainingThread(t *testing.T) {
	testSink := dptest.NewBasicSink()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	drainer := NewDrainingThread(time.Millisecond, []dpsink.Sink{testSink}, []Keeper{&statKeeper{}}, ctx)
	assert.Equal(t, 1, len(drainer.Stats()))
	time.Sleep(time.Millisecond * 10)
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
