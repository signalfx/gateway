package stats

import (
	"testing"
	"time"

	"github.com/signalfuse/signalfxproxy/datapoint"
	"github.com/stretchr/testify/assert"
)

type statKeeper struct {
}

func (m *statKeeper) Stats() []datapoint.Datapoint {
	return []datapoint.Datapoint{nil}
}

type dimensionKeeper struct {
}

func (m *dimensionKeeper) Stats(_ map[string]string) []datapoint.Datapoint {
	return []datapoint.Datapoint{nil}
}

type datapointStreamer struct {
	mychan chan bool
	myDp   chan datapoint.Datapoint
}

func (api *datapointStreamer) Channel() chan<- datapoint.Datapoint {
	api.mychan <- true
	return api.myDp
}
func (api *datapointStreamer) Name() string {
	return ""
}

func TestDrainStatsThread(t *testing.T) {
	c := make(chan bool, 3)
	myDp := make(chan datapoint.Datapoint, 3)
	keepers := []Keeper{&statKeeper{}}
	apis := []datapoint.Streamer{&datapointStreamer{mychan: c, myDp: myDp}}
	// We signal draining thread to die when we get a point
	NewDrainingThread(time.Nanosecond*1, apis, keepers, c).Start()
}

func TestCombineStats(t *testing.T) {
	assert.Equal(t, 2, len(CombineStats([]Keeper{&statKeeper{}, &statKeeper{}})))
}

func TestKeeperWrap(t *testing.T) {
	w := &KeeperWrap{
		Base:       []DimensionKeeper{&dimensionKeeper{}},
		Dimensions: nil,
	}
	assert.Equal(t, 1, len(w.Stats()))
}
