package listener

import (
	"bytes"
	"github.com/cep21/gohelpers/a"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"net/http"
	"testing"
)

func TestMetricTypeFromDsType(t *testing.T) {
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_GAUGE, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("gauge")), "Types don't match expectation")
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_GAUGE, metricTypeFromDsType(nil), "Types don't match expectation")
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_GAUGE, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("unknown")), "Types don't match expectation")
	a.ExpectEquals(t, com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER, metricTypeFromDsType(workarounds.GolangDoesnotAllowPointerToStringLiteral("derive")), "Types don't match expectation")

}

func TestInvalidListen(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:999999"),
	}
	sendTo := &basicDatapointStreamingAPI{}
	_, err := CollectdListenerLoader(sendTo, listenFrom)
	a.ExpectNotNil(t, err)
}

func TestCollectDListener(t *testing.T) {
	jsonBody := `[
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
	sendTo := &basicDatapointStreamingAPI{
		channel: make(chan core.Datapoint),
	}
	listenFrom := &config.ListenFrom{}
	collectdListener, err := CollectdListenerLoader(sendTo, listenFrom)
	defer collectdListener.Close()
	a.ExpectNil(t, err)
	a.ExpectNotNil(t, collectdListener)

	req, _ := http.NewRequest("POST", "http://0.0.0.0:8081/post-collectd", bytes.NewBuffer([]byte(jsonBody)))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	go func() {
		dp := <-sendTo.channel
		a.ExpectEquals(t, "load", dp.Metric(), "Metric not named correctly")

		dp = <-sendTo.channel
		a.ExpectEquals(t, "load", dp.Metric(), "Metric not named correctly")

		dp = <-sendTo.channel
		a.ExpectEquals(t, "load", dp.Metric(), "Metric not named correctly")

		dp = <-sendTo.channel
		a.ExpectEquals(t, "memory", dp.Metric(), "Metric not named correctly")

		dp = <-sendTo.channel
		a.ExpectEquals(t, "df_complex", dp.Metric(), "Metric not named correctly")
	}()
	resp, err := client.Do(req)
	a.ExpectNil(t, err)
	a.ExpectEquals(t, resp.StatusCode, 200, "Request should work")

	a.ExpectEquals(t, 0, len(collectdListener.GetStats()), "Request should work")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:8081/post-collectd", bytes.NewBuffer([]byte(`invalidjson`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	a.ExpectNil(t, err)
	a.ExpectEquals(t, resp.StatusCode, 400, "Request should work")

	req, _ = http.NewRequest("POST", "http://0.0.0.0:8081/post-collectd", bytes.NewBuffer([]byte(jsonBody)))
	req.Header.Set("Content-Type", "application/plaintext")
	resp, err = client.Do(req)
	a.ExpectNil(t, err)
	a.ExpectEquals(t, resp.StatusCode, 400, "Request should work (Plaintext not supported)")

}
