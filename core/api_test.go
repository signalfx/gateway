package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type statKeeper struct {
}

func (m *statKeeper) GetStats() []Datapoint {
	return []Datapoint{nil}
}

type dimensionStatKeeper struct {
}

func (m *dimensionStatKeeper) GetStats(_ map[string]string) []Datapoint {
	return []Datapoint{nil}
}

type datapointStreamingAPI struct {
	mychan chan bool
	myDp   chan Datapoint
}

func (api *datapointStreamingAPI) DatapointsChannel() chan<- Datapoint {
	api.mychan <- true
	return api.myDp
}
func (api *datapointStreamingAPI) Name() string {
	return ""
}

func TestDrainStatsThread(t *testing.T) {
	c := make(chan bool, 3)
	myDp := make(chan Datapoint, 3)
	keepers := []StatKeeper{&statKeeper{}}
	apis := []DatapointStreamingAPI{&datapointStreamingAPI{mychan: c, myDp: myDp}}
	// We signal draining thread to die when we get a point
	NewStatDrainingThread(time.Nanosecond*1, apis, keepers, c).Start()
}

func TestCombineStats(t *testing.T) {
	assert.Equal(t, 2, len(CombineStats([]StatKeeper{&statKeeper{}, &statKeeper{}})))
}

func TestStatKeeperWrap(t *testing.T) {
	w := &StatKeeperWrap{
		Base:       []DimensionStatKeeper{&dimensionStatKeeper{}},
		Dimensions: nil,
	}
	assert.Equal(t, 1, len(w.GetStats()))
}
