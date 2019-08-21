package collectorhandler

import (
	"encoding/json"
	"github.com/signalfx/golib/sfxclient"
	"net/http"
	"strconv"
)

// CollectorHandler collects datapoints
type CollectorHandler struct {
	scheduler *sfxclient.Scheduler
	jsonfunc  func(v interface{}) ([]byte, error)
}

// NewCollectorHandler gets you the new CollectorHandler
func NewCollectorHandler(scheduler *sfxclient.Scheduler) *CollectorHandler {
	return &CollectorHandler{
		scheduler: scheduler,
		jsonfunc:  json.Marshal,
	}
}

// DatapointsHandler exposes a handler func
func (c *CollectorHandler) DatapointsHandler(w http.ResponseWriter, req *http.Request) {
	dps := c.scheduler.CollectDatapoints()
	b, err := c.jsonfunc(dps)
	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		_, err = w.Write(b)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
