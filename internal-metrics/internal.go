package internal

import (
	"encoding/json"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/sfxclient"
	"net/http"
	"strconv"
)

// Collector collects datapoints
type Collector struct {
	scheduler *sfxclient.Scheduler
	logger    log.Logger
	jsonfunc  func(v interface{}) ([]byte, error)
}

// NewCollector gets you the new Collector
func NewCollector(logger log.Logger, scheduler *sfxclient.Scheduler) *Collector {
	return &Collector{
		scheduler: scheduler,
		logger:    logger,
		jsonfunc:  json.Marshal,
	}
}

// MetricsHandler exposes a handler func
func (c *Collector) MetricsHandler(w http.ResponseWriter, req *http.Request) {
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
