package collectd

import (
	"encoding/json"
	"testing"

	"math"
	"math/rand"
	"strconv"

	"github.com/signalfx/gohelpers/workarounds"
	"github.com/signalfx/golib/v3/datapoint"
	"github.com/stretchr/testify/assert"
)

const testDecodeCollectdBody = `[
    {
        "dsnames": [
            "shortterm",
            "midterm",
            "longterm"
        ],
        "dstypes": [
            "gauge",
            "gauge",
            "gauge"
        ],
        "host": "i-b13d1e5f",
        "interval": 10.0,
        "plugin": "load",
        "plugin_instance": "",
        "time": 1415062577.4960001,
        "type": "load",
        "type_instance": "",
        "values": [
            0.37,
            0.60999999999999999,
            0.76000000000000001
        ]
    },
    {
        "dsnames": [
            "value"
        ],
        "dstypes": [
            "gauge"
        ],
        "host": "i-b13d1e5f",
        "interval": 10.0,
        "plugin": "memory",
        "plugin_instance": "",
        "time": 1415062577.4960001,
        "type": "memory",
        "type_instance": "used",
        "values": [
            1524310000.0
        ]
    },
    {
        "dsnames": [
            "value"
        ],
        "dstypes": [
            "derive"
        ],
        "host": "i-b13d1e5f",
        "interval": 10.0,
        "plugin": "df",
        "plugin_instance": "dev",
        "time": 1415062577.4949999,
        "type": "df_complex",
        "type_instance": "free",
        "values": [
            1962600000.0
        ]
    },
    {
        "dsnames": [
            "value"
        ],
        "dstypes": [
            "derive"
        ],
        "host": "i-b13d1e5f",
        "interval": 10.0,
        "plugin": "df",
        "plugin_instance": "dev",
        "time": 1415062577.4949999,
        "type": "",
        "type_instance": "free",
        "values": [
            1962600000.0
        ]
    },
    {
        "dsnames": [
            "value"
        ],
        "dstypes": [
            "derive"
        ],
        "host": "i-b13d1e5f",
        "interval": 10.0,
        "plugin": "ignored",
        "plugin_instance": "ignored2",
        "time": 1415062577.4949999,
        "type": "",
        "type_instance": "free",
        "values": [
            1.7976931348623157e+308
        ]
    },
    {
        "dsnames": [
            "value"
        ],
        "dstypes": [
            "derive"
        ],
        "host": "i-b13d1e5f",
        "interval": 10.0,
        "plugin": "memory",
        "plugin_instance": "",
        "time": 1415062577.4949999,
        "type": "memory",
        "type_instance": "free",
        "values": [
            2.147483647e+09
        ]
    }
]`

const testDecodeCollectdEventBody = `[
	{
		"host": "mwp-signalbox[a=b]",
		"message": "my message",
		"meta": {
			"key": "value"
		},
		"plugin": "my_plugin",
		"plugin_instance": "my_plugin_instance[f=x]",
		"severity": "OKAY",
		"time": 1435104306.0,
		"type": "imanotify",
		"type_instance": "notify_instance[k=v]"
	}
]`

func TestMetricTypeFromDsType(t *testing.T) {
	assert.Equal(t, datapoint.Gauge, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("gauge")), "Types don't match expectation")
	assert.Equal(t, datapoint.Gauge, metricTypeFromDsType(nil), "Types don't match expectation")
	assert.Equal(t, datapoint.Gauge, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("unknown")), "Types don't match expectation")
	assert.Equal(t, datapoint.Counter, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("derive")), "Types don't match expectation")
}

func TestCollectdJsonDecoding(t *testing.T) {
	postFormat := new([]*JSONWriteFormat)
	assert.Nil(t, json.Unmarshal([]byte(testDecodeCollectdBody), &postFormat))
	assert.Equal(t, 6, len(*postFormat))
	emptyMap := map[string]string{}
	dp := NewDatapoint((*postFormat)[0], uint(0), emptyMap)
	assert.Equal(t, "load.shortterm", dp.Metric, "Metric not named correctly")
	dp = NewDatapoint((*postFormat)[1], uint(0), emptyMap)
	assert.Equal(t, "memory.used", dp.Metric, "Metric not named correctly")
	dp = NewDatapoint((*postFormat)[2], uint(0), emptyMap)
	assert.Equal(t, "df_complex.free", dp.Metric, "Metric not named correctly")
	dp = NewDatapoint((*postFormat)[3], uint(0), emptyMap)
	assert.Equal(t, "free", dp.Metric, "Metric not named correctly")
	dp = NewDatapoint((*postFormat)[4], uint(0), emptyMap)
	f := dp.Value.(datapoint.FloatValue).Float()
	assert.Equal(t, math.MaxFloat64, f, "Cannot parse value correctly")

	dp = NewDatapoint((*postFormat)[5], uint(0), emptyMap)
	assert.Equal(t, "memory.free", dp.Metric)
	assert.Equal(t, 3, len(dp.Dimensions))
	i := dp.Value.(datapoint.IntValue).Int()
	assert.Equal(t, int64(math.MaxInt32), i, "Cannot parse value correctly")
}

func TestCollectdEventJsonDecoding(t *testing.T) {
	postFormat := new([]*JSONWriteFormat)
	assert.Nil(t, json.Unmarshal([]byte(testDecodeCollectdEventBody), &postFormat))
	assert.Equal(t, 1, len(*postFormat))
	emptyMap := map[string]string{}
	e := NewEvent((*postFormat)[0], emptyMap)
	assert.Equal(t, "imanotify.notify_instance", e.EventType, "event type not named correctly")
	assert.Equal(t, 3, len(e.Properties), "size of property not corect")
	propertyExists := func(name string, expected string) {
		value, exists := e.Properties[name]
		assert.True(t, exists, "should have "+name+" in property")
		assert.Equal(t, expected, value.(string), name+" should be value "+expected)
	}
	propertyExists("severity", "OKAY")
	propertyExists("message", "my message")
	propertyExists("key", "value")
	assert.Equal(t, 6, len(e.Dimensions), "size of dimensions not corect")
	dimExists := func(name string, expected string) {
		value, exists := e.Dimensions[name]
		assert.True(t, exists, "should have "+name+" in property")
		assert.Equal(t, expected, value, name+" should be value "+expected)
	}
	dimExists("host", "mwp-signalbox")
	dimExists("plugin", "my_plugin")
	dimExists("f", "x")
	dimExists("plugin_instance", "my_plugin_instance")
	dimExists("k", "v")
	dimExists("a", "b")
}

const testDecodeCollectdEventBodyDims = `[
	{
		"host": "mwp-signalbox[a=a,c=c]",
		"message": "my message",
		"meta": {
			"key": "value"
		},
		"plugin": "my_plugin",
		"plugin_instance": "my_plugin_instance[a=b,b=b]",
		"severity": "OKAY",
		"time": 1435104306.0,
		"type": "imanotify",
		"type_instance": "notify_instance[a=c,d=d]"
	}
]`

func TestCollectdDimensonalizedPrecedence(t *testing.T) {
	postFormat := new([]*JSONWriteFormat)
	assert.Nil(t, json.Unmarshal([]byte(testDecodeCollectdEventBodyDims), &postFormat))
	assert.Equal(t, 1, len(*postFormat))
	emptyMap := map[string]string{}
	e := NewEvent((*postFormat)[0], emptyMap)
	assert.Equal(t, "imanotify.notify_instance", e.EventType, "event type not named correctly")
	assert.Equal(t, 3, len(e.Properties), "size of properties not corect")
	propertyExists := func(name string, expected string) {
		value, exists := e.Properties[name]
		assert.True(t, exists, "should have "+name+" in property")
		assert.Equal(t, expected, value.(string), name+" should be value "+expected)
	}
	propertyExists("severity", "OKAY")
	propertyExists("message", "my message")
	propertyExists("key", "value")
	assert.Equal(t, 7, len(e.Dimensions), "size of dimensions not corect")
	dimExists := func(name string, expected string) {
		value, exists := e.Dimensions[name]
		assert.True(t, exists, "should have "+name+" in property")
		assert.Equal(t, expected, value, name+" should be value "+expected)
	}
	dimExists("host", "mwp-signalbox")
	dimExists("plugin", "my_plugin")
	dimExists("plugin_instance", "my_plugin_instance")
	dimExists("a", "c")
	dimExists("b", "b")
	dimExists("c", "c")
	dimExists("d", "d")
}

func TestCollectdParseNameForDimensions(t *testing.T) {
	var tests = []struct {
		val string
		dim map[string]string
	}{
		{"name[k=v,f=x]", map[string]string{"k": "v", "f": "x", "instance": "name"}},
		{"name[k=v,f=x]-rest", map[string]string{"k": "v", "f": "x", "instance": "name-rest"}},
		{"name[k=v,f=x]-middle-[g=h]-more", map[string]string{"k": "v", "f": "x", "instance": "name-middle-[g=h]-more"}},
		{"namek=v,f=x]-middle-[g=h]-more", map[string]string{"g": "h", "instance": "namek=v,f=x]-middle--more"}},
		{"name[k=v,f=x-middle-[g=h]-more", map[string]string{"instance": "name[k=v,f=x-middle-[g=h]-more"}},
		{"[linux=Ubuntu 14.04.5 LTS,release=3.13.0-53-generic,version=#89-Ubuntu SMP Wed May 20 10:34:39 UTC 2015]", map[string]string{"linux": "Ubuntu 14.04.5 LTS", "release": "3.13.0-53-generic", "version": "#89-Ubuntu SMP Wed May 20 10:34:39 UTC 2015"}},
		{"name", map[string]string{"instance": "name"}},
		{"name[k=v", map[string]string{"instance": "name[k=v"}},
		{"user.hit_rate[host:server1,type:production]", map[string]string{"instance": "user.hit_rate[host:server1,type:production]"}},
	}
	for _, test := range tests {
		inputDim := make(map[string]string)
		parseNameForDimensions(inputDim, "instance", &test.val)
		assert.Equal(t, test.dim, inputDim, "Dimensions not equal")
	}
}

func BenchmarkGetDimensionFromNameEmptyDims(b *testing.B) {
	for i := 0; i < b.N; i++ {
		x := "ihavenodims" + strconv.Itoa(rand.Int())
		GetDimensionsFromName(&x)
	}
}

func BenchmarkGetDimensionFromName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		x := "ihave[a=b,c=" + strconv.Itoa(rand.Int()) + "]dims"
		GetDimensionsFromName(&x)
	}
}

func BenchmarkNewDatapoint(b *testing.B) {
	postFormat := new([]*JSONWriteFormat)
	err := json.Unmarshal([]byte(testDecodeCollectdBody), &postFormat)
	if err != nil {
		b.Fail()
	}
	emptyMap := map[string]string{}
	for i := 0; i < b.N; i++ {
		NewDatapoint((*postFormat)[0], uint(0), emptyMap)
		NewDatapoint((*postFormat)[1], uint(0), emptyMap)
		NewDatapoint((*postFormat)[2], uint(0), emptyMap)
		NewDatapoint((*postFormat)[3], uint(0), emptyMap)
		NewDatapoint((*postFormat)[4], uint(0), emptyMap)
		NewDatapoint((*postFormat)[5], uint(0), emptyMap)
	}
}
