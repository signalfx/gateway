package collectd

import (
	"testing"

	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/datapoint/dptest"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
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
	sendTo := dptest.NewBasicSink()
	ctx := context.Background()
	_, err := ListenerLoader(ctx, sendTo, listenFrom)
	assert.Error(t, err)
}

func TestCollectDListener(t *testing.T) {
	jsonBody := testCollectdBody

	sendTo := dptest.NewBasicSink()
	ctx := context.Background()
	listenFrom := &config.ListenFrom{}
	collectdListener, err := ListenerLoader(ctx, sendTo, listenFrom)
	defer collectdListener.Close()
	assert.Nil(t, err)
	assert.NotNil(t, collectdListener)

	req, _ := http.NewRequest("POST", "http://127.0.0.1:8081/post-collectd", bytes.NewBuffer([]byte(jsonBody)))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	go func() {
		dps := <-sendTo.PointsChan
		assert.Equal(t, "load.shortterm", dps[0].Metric, "Metric not named correctly")
		assert.Equal(t, "load.midterm", dps[1].Metric, "Metric not named correctly")
		assert.Equal(t, "load.longterm", dps[2].Metric, "Metric not named correctly")
		assert.Equal(t, "memory.used", dps[3].Metric, "Metric not named correctly")
		assert.Equal(t, "df_complex.free", dps[4].Metric, "Metric not named correctly")
	}()
	resp, err := client.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, resp.StatusCode, http.StatusOK, "Request should work")

	assert.Equal(t, 11, len(collectdListener.Stats()), "Request should work")

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

func TestCollectDListenerWithQueryParams(t *testing.T) {
	jsonBody := testCollectdBody

	sendTo := dptest.NewBasicSink()
	ctx := context.Background()
	c := JSONDecoder{
		SendTo: sendTo,
	}

	req, _ := http.NewRequest("POST", "http://127.0.0.1:8081/post-collectd?sfxdim_foo=bar&sfxdim_zam=narf&sfxdim_empty=&pleaseignore=true", bytes.NewBuffer([]byte(jsonBody)))
	req.Header.Set("Content-Type", "application/json")

	loadExpectedDims := map[string]string{"foo": "bar", "zam": "narf", "plugin": "load", "host": "i-b13d1e5f"}
	memoryExpectedDims := map[string]string{"host": "i-b13d1e5f", "plugin": "memory", "dsname": "value", "foo": "bar", "zam": "narf"}
	dfComplexExpectedDims := map[string]string{"plugin": "df", "plugin_instance": "dev", "dsname": "value", "foo": "bar", "zam": "narf", "host": "i-b13d1e5f"}
	go func() {
		dps := <-sendTo.PointsChan
		assert.Equal(t, "load.shortterm", dps[0].Metric, "Metric not named correctly")
		assert.Equal(t, loadExpectedDims, dps[0].Dimensions, "Dimensions not set correctly")

		assert.Equal(t, "load.midterm", dps[1].Metric, "Metric not named correctly")
		assert.Equal(t, loadExpectedDims, dps[1].Dimensions, "Dimensions not set correctly")

		assert.Equal(t, "load.longterm", dps[2].Metric, "Metric not named correctly")
		assert.Equal(t, loadExpectedDims, dps[2].Dimensions, "Dimensions not set correctly")

		assert.Equal(t, "memory.used", dps[3].Metric, "Metric not named correctly")
		assert.Equal(t, memoryExpectedDims, dps[3].Dimensions, "Dimensions not set correctly")

		assert.Equal(t, "df_complex.free", dps[4].Metric, "Metric not named correctly")
		assert.Equal(t, dfComplexExpectedDims, dps[4].Dimensions, "Dimensions not set correctly")
	}()
	resp := httptest.NewRecorder()
	c.ServeHTTPC(ctx, resp, req)
	assert.Equal(t, resp.Code, http.StatusOK, "Request should work")

	//assert.Equal(t, 4, len(collectdListener.Stats()), "Request should work")

	req, _ = http.NewRequest("POST", "http://127.0.0.1:8081/post-collectd", bytes.NewBuffer([]byte(`invalidjson`)))
	req.Header.Set("Content-Type", "application/json")

	resp = httptest.NewRecorder()
	c.ServeHTTPC(ctx, resp, req)

	assert.Equal(t, c.TotalBlankDims, 1)

	assert.Equal(t, http.StatusBadRequest, resp.Code, "Request should work")

}

func BenchmarkCollectdListener(b *testing.B) {
	bytes := int64(0)
	smallCollectdBody := `[
    {
        "dsnames": [
            "shortterm"
        ],
        "dstypes": [
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
            0.76000000000000001
        ]
    }]`

	sendTo := dptest.NewBasicSink()
	sendTo.PointsChan = make(chan []*datapoint.Datapoint, 2)
	ctx := context.Background()
	c := JSONDecoder{
		SendTo: sendTo,
	}

	b.ReportAllocs()
	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writter := httptest.NewRecorder()
		body := strings.NewReader(smallCollectdBody)
		req, _ := http.NewRequest("GET", "http://example.com/collectd", body)
		req.Header.Add("Content-type", "application/json")
		b.StartTimer()
		c.ServeHTTPC(ctx, writter, req)
		b.StopTimer()
		bytes += int64(len(testCollectdBody))
		item := <-sendTo.PointsChan
		assert.Equal(b, 1, len(item))
	}
	b.SetBytes(bytes)
}
