package collectd

import (
	"testing"

	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"

	"fmt"

	"errors"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/dp/dptest"
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
    },
    {
        "dsnames": [
            "value"
        ],
        "dstypes": [
            "gauge"
        ],
        "host": "mwp-signalbox[a=b]",
        "interval": 10.0,
        "plugin": "tail",
        "plugin_instance": "analytics[f=x]",
        "time": 1434477504.484,
        "type": "memory",
        "type_instance": "old_gen_end[k1=v1,k2=v2]",
        "values": [
            26790
        ]
    },
    {
        "dsnames": [
            "value"
        ],
        "dstypes": [
            "gauge"
        ],
        "host": "mwp-signalbox[a=b]",
        "interval": 10.0,
        "plugin": "tail",
        "plugin_instance": "analytics[f=x]",
        "time": 1434477504.484,
        "type": "memory",
        "type_instance": "total_heap_space[k1=v1,k2=v2]",
        "values": [
            1035520.0
        ]
    },
    {
        "host": "mwp-signalbox",
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
    },
    {
        "time": 1436546167.739,
        "severity": "UNKNOWN",
        "host": "mwp-signalbox",
        "plugin": "tail",
        "plugin_instance": "quantizer",
        "type": "counter",
        "type_instance": "exception[level=error]",
        "message": "the value was found"
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
		assert.Equal(t, len(dps), 7)
		assert.Equal(t, "load.shortterm", dps[0].Metric, "Metric not named correctly")
		assert.Equal(t, "load.midterm", dps[1].Metric, "Metric not named correctly")
		assert.Equal(t, "load.longterm", dps[2].Metric, "Metric not named correctly")
		assert.Equal(t, "memory.used", dps[3].Metric, "Metric not named correctly")
		assert.Equal(t, "df_complex.free", dps[4].Metric, "Metric not named correctly")
		assert.Equal(t, "memory.old_gen_end", dps[5].Metric, "Metric not named correctly")
		assert.Equal(t, "memory.total_heap_space", dps[6].Metric, "Metric not named correctly")
		events := <-sendTo.EventsChan
		assert.Equal(t, len(events), 2)
		assert.Equal(t, "imanotify.notify_instance", events[0].EventType, "Event not named correctly")
		assert.Equal(t, "counter.exception", events[1].EventType, "Event not named correctly")
	}()
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err.Error())
		assert.Fail(t, "Err should be nil")
	}
	assert.Equal(t, http.StatusOK, resp.StatusCode, "Request should work")

	assert.Equal(t, 12, len(collectdListener.Stats()), "Request should work")

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
	parsedInstanceExpectedDims := map[string]string{"zam": "narf", "host": "mwp-signalbox", "plugin_instance": "analytics", "k1": "v1", "k2": "v2", "foo": "bar", "a": "b", "plugin": "tail", "f": "x", "dsname": "value"}
	eventExpectedDims := map[string]string{"foo": "bar", "host": "mwp-signalbox", "plugin": "my_plugin", "f": "x", "plugin_instance": "my_plugin_instance", "k": "v", "zam": "narf"}

	go func() {
		dps := <-sendTo.PointsChan
		assert.Equal(t, "load.shortterm", dps[0].Metric, "Metric not named correctly0")
		assert.Equal(t, loadExpectedDims, dps[0].Dimensions, "Dimensions not set correctly0")

		assert.Equal(t, "load.midterm", dps[1].Metric, "Metric not named correctly1")
		assert.Equal(t, loadExpectedDims, dps[1].Dimensions, "Dimensions not set correctly1")

		assert.Equal(t, "load.longterm", dps[2].Metric, "Metric not named correctly2")
		assert.Equal(t, loadExpectedDims, dps[2].Dimensions, "Dimensions not set correctly2")

		assert.Equal(t, "memory.used", dps[3].Metric, "Metric not named correctly3")
		assert.Equal(t, memoryExpectedDims, dps[3].Dimensions, "Dimensions not set correctly3")

		assert.Equal(t, "df_complex.free", dps[4].Metric, "Metric not named correctly4")
		assert.Equal(t, dfComplexExpectedDims, dps[4].Dimensions, "Dimensions not set correctly4")

		assert.Equal(t, "memory.old_gen_end", dps[5].Metric, "Metric not named correctly5")
		assert.Equal(t, parsedInstanceExpectedDims, dps[5].Dimensions, "Dimensions not set correctly5")

		assert.Equal(t, "memory.total_heap_space", dps[6].Metric, "Metric not named correctly6")
		assert.Equal(t, parsedInstanceExpectedDims, dps[6].Dimensions, "Dimensions not set correctly6")

		events := <-sendTo.EventsChan
		assert.Equal(t, "imanotify.notify_instance", events[0].EventType, "Metric not named correctly event")
		assert.Equal(t, eventExpectedDims, events[0].Dimensions, "Dimensions not set correctly event")
	}()
	resp := httptest.NewRecorder()
	c.ServeHTTPC(ctx, resp, req)
	assert.Equal(t, resp.Code, http.StatusOK, "Request should work")

	req, _ = http.NewRequest("POST", "http://127.0.0.1:8081/post-collectd", bytes.NewBuffer([]byte(`invalidjson`)))
	req.Header.Set("Content-Type", "application/json")

	resp = httptest.NewRecorder()
	c.ServeHTTPC(ctx, resp, req)

	assert.Equal(t, c.TotalBlankDims, int64(1))

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

func TestFailureInRead(t *testing.T) {
	jsonBody := testCollectdBody

	sendTo := dptest.NewBasicSink()
	sendTo.RetError(errors.New("error"))
	ctx := context.Background()
	s := fmt.Sprintf("127.0.0.1:%d", nettest.FreeTCPPort())
	listenFrom := &config.ListenFrom{
		ListenAddr: &s,
	}
	collectdListener, err := ListenerLoader(ctx, sendTo, listenFrom)
	defer collectdListener.Close()
	if err != nil {
		fmt.Printf("%s\n", err.Error())
	}
	assert.Nil(t, err)
	assert.NotNil(t, collectdListener)

	req, _ := http.NewRequest("POST", fmt.Sprintf("http://%s/post-collectd", s), bytes.NewBuffer([]byte(jsonBody)))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "Request should work")
}
