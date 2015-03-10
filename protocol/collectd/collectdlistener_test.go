package collectd

import (
	"bytes"
	"net/http"
	"testing"

	"net/http/httptest"
	"strings"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/datapoint/dptest"
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
    }
]`

func TestInvalidListen(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:999999"),
	}
	sendTo := &dptest.BasicStreamer{}
	_, err := ListenerLoader(sendTo, listenFrom)
	assert.Error(t, err)
}

func TestCollectdListenerInvalidJSONEngine(t *testing.T) {
	sendTo := &dptest.BasicStreamer{
		Chan: make(chan datapoint.Datapoint),
	}
	listenFrom := &config.ListenFrom{
		JSONEngine: workarounds.GolangDoesnotAllowPointerToStringLiteral("unknown"),
	}
	_, err := ListenerLoader(sendTo, listenFrom)
	assert.Error(t, err)
}

func TestCollectDListener(t *testing.T) {
	jsonBody := testCollectdBody

	sendTo := &dptest.BasicStreamer{
		Chan: make(chan datapoint.Datapoint),
	}
	listenFrom := &config.ListenFrom{}
	collectdListener, err := ListenerLoader(sendTo, listenFrom)
	defer collectdListener.Close()
	assert.Nil(t, err)
	assert.NotNil(t, collectdListener)

	req, _ := http.NewRequest("POST", "http://127.0.0.1:8081/post-collectd", bytes.NewBuffer([]byte(jsonBody)))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	go func() {
		dp := <-sendTo.Chan
		assert.Equal(t, "load.shortterm", dp.Metric(), "Metric not named correctly")

		dp = <-sendTo.Chan
		assert.Equal(t, "load.midterm", dp.Metric(), "Metric not named correctly")

		dp = <-sendTo.Chan
		assert.Equal(t, "load.longterm", dp.Metric(), "Metric not named correctly")

		dp = <-sendTo.Chan
		assert.Equal(t, "memory.used", dp.Metric(), "Metric not named correctly")

		dp = <-sendTo.Chan
		assert.Equal(t, "df_complex.free", dp.Metric(), "Metric not named correctly")
	}()
	resp, err := client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK, "Request should work")

	assert.Equal(t, 4, len(collectdListener.Stats()), "Request should work")

	req, _ = http.NewRequest("POST", "http://127.0.0.1:8081/post-collectd", bytes.NewBuffer([]byte(`invalidjson`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "Request should work")

	req, _ = http.NewRequest("POST", "http://127.0.0.1:8081/post-collectd", bytes.NewBuffer([]byte(jsonBody)))
	req.Header.Set("Content-Type", "application/plaintext")
	resp, err = client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode, "Request should work (Plaintext not supported)")

}

func BenchmarkCollectdListener(b *testing.B) {
	bytes := int64(0)

	jsonEngine, _ := LoadEngine("")

	for i := 0; i < b.N; i++ {
		sendTo := &dptest.BasicStreamer{
			Chan: make(chan datapoint.Datapoint, 6),
		}

		decoder := JSONDecoder{
			DecodingEngine: jsonEngine,
			DatapointTracker: datapoint.Tracker{
				Streamer: sendTo,
			},
		}
		writter := httptest.NewRecorder()
		body := strings.NewReader(testCollectdBody)
		req, err := http.NewRequest("GET", "http://example.com/collectd", body)
		req.Header.Add("Content-type", "application/json")
		decoder.ServeHTTP(writter, req)
		bytes += int64(len(testCollectdBody))
		//		listener.handleCollectd(writter, req)
		assert.NoError(b, err)
		assert.Equal(b, 5, len(sendTo.Chan))
	}
	b.SetBytes(bytes)
}
