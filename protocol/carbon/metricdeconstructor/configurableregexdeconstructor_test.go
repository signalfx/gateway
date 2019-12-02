package metricdeconstructor

import (
	"encoding/json"
	"github.com/signalfx/golib/datapoint"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

var validRegexConfig = `{
  "MetricRules": [
	{
	  "Regex": "(?P<sf_metric_0>foo.*)\\.(?P<middle>.*)(?P<sf_metric_1>\\.baz)",
	  "AdditionalDimensions": {
		"key1": "value1"
	  }
	},
	{
	  "Regex": "(?P<sf_metric>umbop.*)",
      "MetricType": "cumulative_counter"
	},
	{
	  "Regex": "blarg.*"
	},
	{
	  "Regex": "madeup.*",
	  "MetricName": "madeup.blarg"
	}
  ]
}`

func getRegexData(t *testing.T, data string) map[string]interface{} {
	dat := make(map[string]interface{}, 0)
	err := json.Unmarshal([]byte(data), &dat)
	assert.NoError(t, err)
	return dat
}

func getRegexDeconstructor(t *testing.T) MetricDeconstructor {
	m, err := regexJSONLoader(getRegexData(t, validRegexConfig))
	assert.NoError(t, err)
	return m
}

func TestRegexParser(t *testing.T) {
	Convey("Given the valid config", t, func() {
		m := getRegexDeconstructor(t)
		Convey("line with three terms matching foo.bar.baz", func() {
			metric, metricType, dimensions, err := m.Parse("foo.bar.baz")
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("metric should be the beginning and the end", func() {
				So(metric, ShouldEqual, "foo.baz")
			})
			Convey("should be a gauge", func() {
				So(metricType, ShouldEqual, datapoint.Gauge)
			})
			Convey("hould have two dimensions", func() {
				So(len(dimensions), ShouldEqual, 2)
				So(dimensions["key1"], ShouldEqual, "value1")
				So(dimensions["middle"], ShouldEqual, "bar")
			})
		})
		Convey("line matching matching metric but not dimensions", func() {
			metric, metricType, dimensions, err := m.Parse("baz.bar.foo")
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("metric end up at fallback", func() {
				So(metric, ShouldEqual, "baz.bar.foo")
			})
			Convey("should be a gauge", func() {
				So(metricType, ShouldEqual, datapoint.Gauge)
			})
			Convey("hould have two dimensions", func() {
				So(len(dimensions), ShouldEqual, 0)
			})
		})
		Convey("line with four terms which matches", func() {
			metric, metricType, dimensions, err := m.Parse("foo.dippity.bar.baz")
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("metric should be the everything but the .bar", func() {
				So(metric, ShouldEqual, "foo.dippity.baz")
			})
			Convey("should be a gauge", func() {
				So(metricType, ShouldEqual, datapoint.Gauge)
			})
			Convey("should have two dimensions and middle should be everything but foo and baz", func() {
				So(len(dimensions), ShouldEqual, 2)
				So(dimensions["key1"], ShouldEqual, "value1")
				So(dimensions["middle"], ShouldEqual, "bar")
			})
		})
		Convey("line that starts with umbop", func() {
			metric, metricType, dimensions, err := m.Parse("umbop.hansen.blarg")
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("metric should be everything", func() {
				So(metric, ShouldEqual, "umbop.hansen.blarg")
			})
			Convey("should be a counter", func() {
				So(metricType, ShouldEqual, datapoint.Counter)
			})
			Convey("should have no dimensions", func() {
				So(len(dimensions), ShouldEqual, 0)
			})
		})
		Convey("line that starts with blarg", func() {
			metric, metricType, dimensions, err := m.Parse("blarg.i.am.not.going.to.school")
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("metric should be the whole thing since no metric was specified", func() {
				So(metric, ShouldEqual, "blarg.i.am.not.going.to.school")
			})
			Convey("should be a gauge", func() {
				So(metricType, ShouldEqual, datapoint.Gauge)
			})
			Convey("should have no dimensions", func() {
				So(len(dimensions), ShouldEqual, 0)
			})
		})
		Convey("line that starts with madeup", func() {
			metric, metricType, dimensions, err := m.Parse("madeup.i.am.not.going.to.school")
			Convey("should parse without error", func() {
				So(err, ShouldBeNil)
			})
			Convey("metric should be what we set with no additions", func() {
				So(metric, ShouldEqual, "madeup.blarg")
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

func TestRegexFallback(t *testing.T) {
	var fallback = `{
  "FallbackDeconstructor":"nil",
  "MetricRules": []
}`
	Convey("Given the config specifying the nil deconstructor", t, func() {
		m, err := regexJSONLoader(getRegexData(t, fallback))
		Convey("should parse the config without error", func() {
			So(err, ShouldBeNil)
		})
		Convey("line with seven terms", func() {
			_, _, _, err = m.Parse("one.two.three.four.five.six.seven")
			Convey("should parse giving an error", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, SkipMetricErr)
			})
		})
	})
	var badFallback = `{
  "FallbackDeconstructor":"blarg",
  "MetricRules": []
}`
	Convey("Given the config specifying the bad deconstructor", t, func() {
		m, err := regexJSONLoader(getRegexData(t, badFallback))
		Convey("should parse the config without error", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err.Error(), ShouldEqual, "unable to load metric deconstructor by the name of blarg")
		})
	})
	var badFallbackConfig = `{
  "FallbackDeconstructor": "commakeys",
  "FallbackDeconstructorConfig": "blarg",
  "MetricRules": []
}`
	Convey("Given the bad config with bad fallback config", t, func() {
		m, err := regexJSONLoader(getData(t, badFallbackConfig))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err.Error(), ShouldEqual, "unknown commaKeysLoaderDeconstructor parameter blarg")
		})
	})
}

func TestBadRegexConfig(t *testing.T) {
	var noMetricName = `{
  "MetricRules": [
	{
	  "Regex": ".*",
	  "MetricType": "blarg"
	}
  ]
}`
	Convey("Given the bad config with bad metricType", t, func() {
		m, err := regexJSONLoader(getData(t, noMetricName))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err.Error(), ShouldEqual, "cannot parse configured MetricType of blarg")
		})
	})
	var invalidMapToObject = `{
  "MetricRules": [
	{
	  "Regex": ".*",
	  "AdditionalDimensions": "blarg"
	}
  ]
}`
	Convey("Given the bad config with bad json", t, func() {
		m, err := regexJSONLoader(getData(t, invalidMapToObject))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(strings.HasPrefix(err.Error(), "json: cannot unmarshal string"), ShouldBeTrue)
		})
	})
	Convey("Given the bad config with bad json", t, func() {
		m, err := regexJSONLoader(map[string]interface{}{"noparse": func() {}})
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err.Error(), ShouldEqual, "json: unsupported type: func()")
		})
	})
	var invalidCompile = `{
  "MetricRules": [
	{
	  "Regex": "[abc"
	}
  ]
}`
	Convey("Given the bad config with bad json", t, func() {
		m, err := regexJSONLoader(getData(t, invalidCompile))
		Convey("should parse the config and err", func() {
			So(err, ShouldNotBeNil)
			So(m, ShouldBeNil)
			So(err.Error(), ShouldEqual, "error parsing regexp: missing closing ]: `[abc`")
		})
	})
}
