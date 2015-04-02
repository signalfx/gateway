package collectd

import (
	"encoding/json"
	"testing"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/metricproxy/datapoint"
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
	assert.Equal(t, com_signalfx_metrics_protobuf.MetricType_GAUGE, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("gauge")), "Types don't match expectation")
	assert.Equal(t, com_signalfx_metrics_protobuf.MetricType_GAUGE, metricTypeFromDsType(nil), "Types don't match expectation")
	assert.Equal(t, com_signalfx_metrics_protobuf.MetricType_GAUGE, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("unknown")), "Types don't match expectation")
	assert.Equal(t, com_signalfx_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("derive")), "Types don't match expectation")
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
