package metricdeconstructor

import (
	"testing"

	"encoding/json"
	"fmt"

	"github.com/signalfx/golib/datapoint"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"strings"
)

var validConfig = `{
  "TypeRules": [
    {
      "StartsWith":"counter",
      "MetricType": "count"
    },
    {
      "EndsWith":"count",
      "MetricType": "count"
    }
  ],
  "MetricRules": [
	{
	  "MetricPath": "email|mail.account.*.*.*.*.*",
	  "DimensionsMap": "object.object.-.action.action.%.%",
	  "MetricType": "count",
	  "Dimensions": {
		"key1": "value1",
		"key2": "value2"
	  }
	},
	{
	  "DimensionsMap": "service.instance.thang.%",
	  "MetricName": "thinger"
	},
	{
	  "MetricPath": "cassandra",
	  "DimensionsMap": "service.occurrence.%"
	},
	{
	  "MetricPath": "!cassandra",
	  "DimensionsMap": "service.instance.%.%.-"
	},
	{
	  "DimensionsMap": "service.instance.%"
	}
  ]
}`

func getData(t *testing.T, data string) map[string]interface{} {
	dat := make(map[string]interface{}, 0)
	err := json.Unmarshal([]byte(data), &dat)
	assert.NoError(t, err)
	return dat
}

func getDeconstructor(t *testing.T) MetricDeconstructor {
	m, err := delimiterJSONLoader(getData(t, validConfig))
	assert.NoError(t, err)
	return m
}

// this tests many things in one
// - joined metric names
// - joined dimensions
// - extra dimensions
// - changing metric type from default
// - ignoring a dimension
// - test or as well
func TestParser(t *testing.T) {
	Convey("Given the valid config", t, func() {
		m := getDeconstructor(t)
		Convey("seven term line starting with email.account", func() {
			metric, metricType, dimensions, err := m.Parse("email.account.welcome_new.click.it-IT.gmail.count")
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("should parse with a joined metricName", func() {
				So(metric, ShouldEqual, "gmail.count")
			})
			Convey("should be a counter", func() {
				So(metricType, ShouldEqual, datapoint.Count)
			})
			dims := map[string]string{"object": "email.account", "action": "click.it-IT", "key1": "value1", "key2": "value2"}
			Convey("action and object should be joined and supplemental dimensions were added", func() {
				So(dimensions, ShouldResemble, dims)
			})
		})
		Convey("String should work", func() {
			s := fmt.Sprintf("%v", m)
			So(s, ShouldNotBeNil)
		})
		Convey("seven term line starting with mail.account", func() {
			metric, metricType, dimensions, err := m.Parse("mail.account.welcome_new.click.it-IT.mail.count")
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("should parse with a joined metricName", func() {
				So(metric, ShouldEqual, "mail.count")
			})
			Convey("should be a counter", func() {
				So(metricType, ShouldEqual, datapoint.Count)
			})
			dims := map[string]string{"object": "mail.account", "action": "click.it-IT", "key1": "value1", "key2": "value2"}
			Convey("action and object should be joined and supplemental dimensions were added and some ignored", func() {
				So(dimensions, ShouldResemble, dims)
			})
		})
		Convey("generic line with three terms", func() {
			metric, metricType, dimensions, err := m.Parse("gmail.gmail23.invalid_logins")
			Convey("should parse without error in config not needing a metric_path", func() {
				So(err, ShouldBeNil)
			})
			Convey("last field should be metricName", func() {
				So(metric, ShouldEqual, "invalid_logins")
			})
			Convey("should be a gauge", func() {
				So(metricType, ShouldEqual, datapoint.Gauge)
			})
			dims := map[string]string{"service": "gmail", "instance": "gmail23"}
			Convey("first two fields should be dimensions", func() {
				So(dimensions, ShouldResemble, dims)
			})
		})
		Convey("line with three terms start with cassandra", func() {
			metric, metricType, dimensions, err := m.Parse("cassandra.cassandra23.invalid_logins")
			Convey("should parse without error even though only cassandra is specified", func() {
				So(err, ShouldBeNil)
			})
			Convey("last field should be metricName", func() {
				So(metric, ShouldEqual, "invalid_logins")
			})
			Convey("should be a gauge", func() {
				So(metricType, ShouldEqual, datapoint.Gauge)
			})
			dims := map[string]string{"service": "cassandra", "occurrence": "cassandra23"}
			Convey("first two fields should be dimensions", func() {
				So(dimensions, ShouldResemble, dims)
			})
		})
		Convey("line with five terms starting with cassandra", func() {
			metric, metricType, dimensions, err := m.Parse("cassandra.cassandra23.invalid_logins.rate.ignored")
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("should have fallen through to identify graphite deconstructor", func() {
				So(metric, ShouldEqual, "cassandra.cassandra23.invalid_logins.rate.ignored")
			})
			Convey("should be a gauge", func() {
				So(metricType, ShouldEqual, datapoint.Gauge)
			})
			Convey("hould have no dimensions", func() {
				So(len(dimensions), ShouldEqual, 0)
			})
		})
		Convey("line with five terms not starting with cassandra", func() {
			metric, metricType, dimensions, err := m.Parse("notcassandra.cassandra23.invalid_logins.rate.ignored")
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("should not have fallen though so metric should be last two terms", func() {
				So(metric, ShouldEqual, "invalid_logins.rate")
			})
			Convey("should be a gauge", func() {
				So(metricType, ShouldEqual, datapoint.Gauge)
			})
			dims := map[string]string{"service": "notcassandra", "instance": "cassandra23"}
			Convey("should parse dimensions correctly", func() {
				So(dimensions, ShouldResemble, dims)
			})
		})
		Convey("line with four terms", func() {
			metric, metricType, dimensions, err := m.Parse("gmail.gmail23.invalid_logins.rate")
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("specified metric should be appended with last term", func() {
				So(metric, ShouldEqual, "thinger.rate")
			})
			Convey("should be a gauge", func() {
				So(metricType, ShouldEqual, datapoint.Gauge)
			})
			dims := map[string]string{"service": "gmail", "instance": "gmail23", "thang": "invalid_logins"}
			Convey("pull the rest of the dimensions out", func() {
				So(dimensions, ShouldResemble, dims)
			})
		})
		Convey("line with four terms starting with counter", func() {
			metric, metricType, dimensions, err := m.Parse("counter.gmail23.invalid_logins.rate")
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("specified metric should be appended with last term", func() {
				So(metric, ShouldEqual, "thinger.rate")
			})
			Convey("should be a counter because of type rule starting with counter", func() {
				So(metricType, ShouldEqual, datapoint.Count)
			})
			dims := map[string]string{"service": "counter", "instance": "gmail23", "thang": "invalid_logins"}
			Convey("pull the rest of the dimensions out", func() {
				So(dimensions, ShouldResemble, dims)
			})
		})
		Convey("line with four terms ending with count", func() {
			metric, metricType, dimensions, err := m.Parse("gmail.gmail23.invalid_logins.count")
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("specified metric should be appended with last term", func() {
				So(metric, ShouldEqual, "thinger.count")
			})
			Convey("should be a counter because of type rule ending with count", func() {
				So(metricType, ShouldEqual, datapoint.Count)
			})
			dims := map[string]string{"service": "gmail", "instance": "gmail23", "thang": "invalid_logins"}
			Convey("pull the rest of the dimensions out", func() {
				So(dimensions, ShouldResemble, dims)
			})
		})
		Convey("line with eight terms", func() {
			graphiteDim := "one.two.three.four.five.six.seven.eight"
			metric, metricType, dimensions, err := m.Parse(graphiteDim)
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("metric should be the whole graphite metric due to no 8 term rule", func() {
				So(metric, ShouldEqual, graphiteDim)
			})
			Convey("should be a gauge", func() {
				So(metricType, ShouldEqual, datapoint.Gauge)
			})
			Convey("should have no dimensions", func() {
				So(len(dimensions), ShouldEqual, 0)
			})
		})
	})
}

func TestFallbackNil(t *testing.T) {
	var fallback = `{
  "FallbackDeconstructor":"nil",
  "MetricRules": []
}`
	Convey("Given the config specifying the nil deconstructor", t, func() {
		m, err := delimiterJSONLoader(getData(t, fallback))
		Convey("should parse the config without error", func() {
			So(err, ShouldBeNil)
		})
		Convey("line with seven terms", func() {
			_, _, _, err = m.Parse("one.two.three.four.five.six.seven")
			Convey("should parse giving an error", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, ErrSkipMetric)
			})
		})
	})
}

func TestBadConfigDifferentSizes(t *testing.T) {
	var differentSizes = `{
  "MetricRules": [
	{
	  "MetricPath": "*.*.*.*",
	  "DimensionsMap": "service.instance.%"
	}
  ]
}`
	Convey("Given the bad config with metricpath having more terms than dimensionsmap", t, func() {
		m, err := delimiterJSONLoader(getData(t, differentSizes))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err.Error(), ShouldEqual, "the MetricPath *.*.*.* has 4 terms but DimensionsMap service.instance.% has 3")
		})
	})
}

func TestBadConfigNoMetricName(t *testing.T) {
	var noMetricName = `{
  "MetricRules": [
	{
	  "MetricPath": "*.*.*",
	  "DimensionsMap": "service.instance.-"
	}
  ]
}`
	Convey("Given the bad config with no metricName", t, func() {
		m, err := delimiterJSONLoader(getData(t, noMetricName))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err.Error(), ShouldEqual, "the DimensionsMap does not have a metric specified; use % to specify the metric name override MetricName")
		})
	})
}

func TestBadConfigBadMetricType(t *testing.T) {
	var badMetricType = `{
  "MetricRules": [
	{
	  "MetricPath": "*.*.*",
	  "DimensionsMap": "service.instance.%",
	  "MetricType":"blarg"
	}
  ]
}`
	Convey("Given the bad config with bad metricType", t, func() {
		m, err := delimiterJSONLoader(getData(t, badMetricType))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err.Error(), ShouldEqual, "cannot parse configured MetricType of blarg")
		})
	})
}

func TestBadFallbackDeconstructor(t *testing.T) {
	var badFallback = `{
  "FallbackDeconstructor": "blarg",
  "MetricRules": [
	{
	  "MetricPath": "*.*.*",
	  "DimensionsMap": "service.instance.%"
	}
  ]
}`
	Convey("Given the bad config with bad fallback", t, func() {
		m, err := delimiterJSONLoader(getData(t, badFallback))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err.Error(), ShouldEqual, "unable to load metric deconstructor by the name of blarg")
		})
	})
}

func TestBadFallbackDeconstructorConfig(t *testing.T) {
	var badFallback = `{
  "FallbackDeconstructor": "commakeys",
  "FallbackDeconstructorConfig": "blarg",
  "MetricRules": [
	{
	  "MetricPath": "*.*.*",
	  "DimensionsMap": "service.instance.%"
	}
  ]
}`
	Convey("Given the bad config with bad fallback config", t, func() {
		m, err := delimiterJSONLoader(getData(t, badFallback))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err.Error(), ShouldEqual, "unknown commaKeysLoaderDeconstructor parameter blarg")
		})
	})
}

func TestInvalidMapToObject(t *testing.T) {
	var invalidMapToObject = `{
  "MetricRules": [
	{
	  "MetricPath": 4,
	  "DimensionsMap": "service.instance.%"
	}
  ]
}`
	Convey("Given the bad config with bad json", t, func() {
		m, err := delimiterJSONLoader(getData(t, invalidMapToObject))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(strings.HasPrefix(err.Error(), "json: cannot unmarshal number"), ShouldBeTrue)
		})
	})
}

func TestNoType(t *testing.T) {
	var noType = `{
  "TypeRules": [
    {
      "StartsWith":"counter",
      "EndsWith":"count"
    }
  ],
  "MetricRules": []
}`
	Convey("Given the bad config with no type", t, func() {
		m, err := delimiterJSONLoader(getData(t, noType))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err, ShouldEqual, errTypeRuleNotDefined)
		})
	})
}

func TestNoRules(t *testing.T) {
	var noRules = `{
  "TypeRules": [
    {
      "MetricType": "count"
    }
  ],
  "MetricRules": []
}`
	Convey("Given the bad config with no type", t, func() {
		m, err := delimiterJSONLoader(getData(t, noRules))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err, ShouldEqual, errTypeRuleIllDefined)
		})
	})
}

func TestInvalidType(t *testing.T) {
	var invalidType = `{
  "TypeRules": [
    {
      "MetricType": "blarg",
      "StartsWith":"counter",
      "EndsWith":"count"
    }
  ],
  "MetricRules": []
}`
	Convey("Given the bad config with invalid type", t, func() {
		m, err := delimiterJSONLoader(getData(t, invalidType))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err.Error(), ShouldEqual, "cannot parse configured MetricType of blarg")
		})
	})
}

func TestDuplicateDelimiter(t *testing.T) {
	var duplicateDelimiter = `{
  "OrDelimiter":".",
  "MetricRules": []
}`
	Convey("Given the bad config with invalid type", t, func() {
		m, err := delimiterJSONLoader(getData(t, duplicateDelimiter))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err.Error(), ShouldEqual, "overridden option cannot be the same as another: .")
		})
	})
}

func TestOverrideEverything(t *testing.T) {
	var overrideEverything = `{
  "OrDelimiter":"*",
  "NotDelimiter":"-",
  "Delimiter":"%",
  "Globbing":"|",
  "IgnoreDimension":"!",
  "MetricIdentifier":".",
  "Dimensions": { "key":"value"},
  "MetricRules": [
	{
	  "DimensionsMap": "service%instance%."
	}
  ]
}`
	Convey("Given the config with everything overridden", t, func() {
		m, err := delimiterJSONLoader(getData(t, overrideEverything))
		Convey("should parse the config without errors", func() {
			So(m, ShouldNotBeNil)
			So(err, ShouldBeNil)
			Convey("a line with 3 terms and with % as a delimiter", func() {
				metric, metricType, dimensions, err := m.Parse("cassandra%cassandra23%invalid_logins")
				Convey("should parse without error", func() {
					So(err, ShouldBeNil)
				})
				Convey("metric should last term", func() {
					So(metric, ShouldEqual, "invalid_logins")
				})
				Convey("should be a gauge", func() {
					So(metricType, ShouldEqual, datapoint.Gauge)
				})
				dims := map[string]string{"service": "cassandra", "instance": "cassandra23", "key": "value"}
				Convey("should have no dimensions", func() {
					So(dimensions, ShouldResemble, dims)
				})
			})
		})
	})
}
