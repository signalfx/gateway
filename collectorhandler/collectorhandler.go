package collectorhandler

import (
	"encoding/json"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/sfxclient"
	"net/http"
	"strconv"
)

// CollectorHandler collects datapoints
type CollectorHandler struct {
	schedulers         []*sfxclient.Scheduler
	jsonMarshallerFunc func(v interface{}) ([]byte, error)
}

// NewCollectorHandler gets you the new CollectorHandler
func NewCollectorHandler(schedulers ...*sfxclient.Scheduler) *CollectorHandler {
	return &CollectorHandler{
		schedulers:         schedulers,
		jsonMarshallerFunc: json.Marshal,
	}
}

// DatapointsHandler exposes a handler func
func (c *CollectorHandler) DatapointsHandler(w http.ResponseWriter, req *http.Request) {
	dps := make([]*datapoint.Datapoint, 0, len(c.schedulers))
	for _, scheduler := range c.schedulers {
		dps = append(dps, scheduler.CollectDatapoints()...)
	}
	b, err := c.jsonMarshallerFunc(dps)
	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		_, err = w.Write(b)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
