package core

import (
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

type statKeeper struct {
	mock.Mock
}

func (m *statKeeper) GetStats() []Datapoint {
	return []Datapoint{nil}
}

type datapointStreamingAPI struct {
	mock.Mock
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
