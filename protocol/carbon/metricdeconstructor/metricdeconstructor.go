package metricdeconstructor

import (
	"fmt"

	"github.com/signalfx/golib/datapoint"
)

// MetricDeconstructor is an object that can deconstruct a single metric name into what dimensions
// it should represent.  Useful for compatibility with non dimensioned stores, like graphite
type MetricDeconstructor interface {
	Parse(originalMetric string) (newMetric string, mtype datapoint.MetricType, dimension map[string]string, err error)
}

type loader func(string) (MetricDeconstructor, error)
type loadJSON func(map[string]interface{}) (MetricDeconstructor, error)

var knownLoaders = map[string]loader{
	"":          identityLoader,
	"identity":  identityLoader,
	"datadog":   commaKeysLoader,
	"commakeys": commaKeysLoader,
	"nil":       nilLoader,
}

// Load will load a MetricDeconstructor of the given name, with the given options
func Load(name string, options string) (MetricDeconstructor, error) {
	loader, exists := knownLoaders[name]
	if !exists {
		return nil, fmt.Errorf("unable to load metric deconstructor by the name of %s", name)
	}
	return loader(options)
}

var knownJSONLoaders = map[string]loadJSON{
	"delimiter": delimiterJSONLoader,
}

// LoadJSON will load a MetricDeconstructor of the given name, with the given JSON object
func LoadJSON(name string, options map[string]interface{}) (MetricDeconstructor, error) {
	loader, exists := knownJSONLoaders[name]
	if !exists {
		return nil, fmt.Errorf("unable to load from json metric deconstructor by the name of %s", name)
	}
	return loader(options)
}
