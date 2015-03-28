package datapoint

import (
	"os"

	"time"

	log "github.com/Sirupsen/logrus"
)

var osXXXHostname = os.Hostname

// NewOnHostDatapoint is like NewSingleNameDataPointWithType but also a source
// of this host
func NewOnHostDatapoint(metric string, value Value,
	metricType MetricType) *Datapoint {
	return NewOnHostDatapointDimensions(metric, value, metricType, map[string]string{})
}

// NewOnHostDatapointDimensions is like NewOnHostDatapoint but also with optional dimensions
func NewOnHostDatapointDimensions(metric string, value Value, metricType MetricType,
	dimensions map[string]string) *Datapoint {
	hostname, err := osXXXHostname()
	if err != nil {
		log.WithField("err", err).Warn("Unable to find hostname")
		hostname = "unknown"
	}
	dimensions["host"] = hostname
	dimensions["source"] = "proxy"
	return New(metric, dimensions, value, metricType, time.Now())
}
