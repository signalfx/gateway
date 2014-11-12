package protocoltypes

import (
	"encoding/json"
	"github.com/cep21/gohelpers/a"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"testing"
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
    }
]`

func TestMetricTypeFromDsType(t *testing.T) {
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_GAUGE, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("gauge")), "Types don't match expectation")
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_GAUGE, metricTypeFromDsType(nil), "Types don't match expectation")
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_GAUGE, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("unknown")), "Types don't match expectation")
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("derive")), "Types don't match expectation")
}

func TestCollectdJsonDecoding(t *testing.T) {
	var postFormat CollectdJSONWriteBody
	a.ExpectNil(t, json.Unmarshal([]byte(testCollectdBody), &postFormat))
	dp := NewCollectdDatapoint(postFormat[0], uint(0))
	a.ExpectEquals(t, "load.shortterm", dp.Metric(), "Metric not named correctly")
	dp = NewCollectdDatapoint(postFormat[2], uint(0))
	a.ExpectEquals(t, "df_complex.free", dp.Metric(), "Metric not named correctly")
}
