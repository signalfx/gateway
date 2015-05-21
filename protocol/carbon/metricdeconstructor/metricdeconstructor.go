package metricdeconstructor

import (
	"fmt"

	"github.com/signalfx/golib/datapoint"
)

// MetricDeconstructor is an object that can deconstruct a single metric name into what dimensions
// it should represent.  Useful for compatability with non dimentioned stores, like graphite
type MetricDeconstructor interface {
	Parse(originalMetric string) (newMetric string, mtype datapoint.MetricType, dimension map[string]string, err error)
}

type loader func(string) (MetricDeconstructor, error)

var knownLoaders = map[string]loader{
	"":          identityLoader,
	"identity":  identityLoader,
	"datadog":   commaKeysLoader,
	"commakeys": commaKeysLoader,
}

// Load will load a MetricDeconstructor of the given name, with the given options
func Load(name string, options string) (MetricDeconstructor, error) {
	loader, exists := knownLoaders[name]
	if !exists {
		return nil, fmt.Errorf("unable to load metric deconstructor by the name of %s", name)
	}
	return loader(options)
}
