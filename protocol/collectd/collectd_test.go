package collectd

import (
	"encoding/json"
	"testing"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/golib/datapoint"
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
            5.36202e+09
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
            5.36202e+09
        ]
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
	assert.Equal(t, 5.36202e+09, f, "Cannot parse value correctly")

	dp = NewDatapoint((*postFormat)[5], uint(0), emptyMap)
	assert.Equal(t, "memory.free", dp.Metric)
	assert.Equal(t, 3, len(dp.Dimensions))
}

func TestCollectdParseInstanceNameForDimensions(t *testing.T) {
	var tests = []struct {
		val string
		dim map[string]string
	}{
		{"name[k=v,f=x]", map[string]string{"k": "v", "f": "x", "instance": "name"}},
		{"name[k=v,f=x]-rest", map[string]string{"k": "v", "f": "x", "instance": "name-rest"}},
		{"name[k=v,f=x]-middle-[g=h]-more", map[string]string{"k": "v", "f": "x", "instance": "name-middle-[g=h]-more"}},
		{"namek=v,f=x]-middle-[g=h]-more", map[string]string{"g": "h", "instance": "namek=v,f=x]-middle--more"}},
		{"name[k=v,f=x-middle-[g=h]-more", map[string]string{"instance": "name[k=v,f=x-middle-[g=h]-more"}},
		{"name", map[string]string{"instance": "name"}},
		{"name[k=v", map[string]string{"instance": "name[k=v"}},
	}
	for _, test := range tests {
		inputDim := make(map[string]string)
		parseInstanceNameForDimensions(inputDim, "instance", true, &test.val)
		assert.Equal(t, test.dim, inputDim, "Dimensions not equal")
	}

}
