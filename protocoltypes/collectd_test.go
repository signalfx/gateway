package protocoltypes

import (
	"encoding/json"
	"testing"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/stretchr/testify/assert"
)

const testCollectdBody = `[
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
    }
]`

func TestMetricTypeFromDsType(t *testing.T) {
	assert.Equal(t, com_signalfuse_metrics_protobuf.MetricType_GAUGE, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("gauge")), "Types don't match expectation")
	assert.Equal(t, com_signalfuse_metrics_protobuf.MetricType_GAUGE, metricTypeFromDsType(nil), "Types don't match expectation")
	assert.Equal(t, com_signalfuse_metrics_protobuf.MetricType_GAUGE, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("unknown")), "Types don't match expectation")
	assert.Equal(t, com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("derive")), "Types don't match expectation")
}

func TestCollectdJsonDecoding(t *testing.T) {
	//	var postFormat CollectdJSONWriteBody
	postFormat := new([]*CollectdJSONWriteFormat)
	assert.Nil(t, json.Unmarshal([]byte(testCollectdBody), &postFormat))
	//	assert.Nil(t, NewCollectdJSONWriteFormatJSONDecoder(bytes.NewBuffer([]byte(testCollectdBody))).DecodeArray(postFormat))
	assert.Equal(t, 5, len(*postFormat), "Expect 5 elements")
	dp := NewCollectdDatapoint((*postFormat)[0], uint(0))
	assert.Equal(t, "load.shortterm", dp.Metric(), "Metric not named correctly")
	dp = NewCollectdDatapoint((*postFormat)[1], uint(0))
	assert.Equal(t, "memory.used", dp.Metric(), "Metric not named correctly")
	dp = NewCollectdDatapoint((*postFormat)[2], uint(0))
	assert.Equal(t, "df_complex.free", dp.Metric(), "Metric not named correctly")
	dp = NewCollectdDatapoint((*postFormat)[3], uint(0))
	assert.Equal(t, "free", dp.Metric(), "Metric not named correctly")
	dp = NewCollectdDatapoint((*postFormat)[4], uint(0))
	f, _ := dp.Value().FloatValue()
	assert.Equal(t, 5.36202e+09, f, "Cannot parse value correctly")
}
