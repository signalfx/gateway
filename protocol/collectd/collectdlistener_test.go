package collectd

import (
	"bytes"
	"fmt"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/web"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
	"net/http"
	"net/http/httptest"
	"strings"
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
        "dsnames": [
            "value"
        ],
        "dstypes": [
            "gauge"
        ],
        "host": "some-host",
        "interval": 10.0,
        "plugin": "dogstatsd",
        "plugin_instance": "[env=dev,k1=v1]",
        "time": 1434477504.484,
        "type": "gauge",
        "type_instance": "page.loadtime",
        "values": [
            12.0
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

func TestCollectDListener(t *testing.T) {
	Convey("invalid listener host should fail to connect", t, func() {
		conf := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:99999999r"),
		}
		sendTo := dptest.NewBasicSink()

		_, err := NewListener(sendTo, conf)
		So(err, ShouldNotBeNil)
	})
	Convey("a basic collectd listener", t, func() {
		debugContext := &web.HeaderCtxFlag{
			HeaderName: "X-Test",
		}
		conf := &ListenerConfig{
			ListenAddr:   pointer.String("127.0.0.1:0"),
			DebugContext: debugContext,
		}
		sendTo := dptest.NewBasicSink()

		listener, err := NewListener(sendTo, conf)
		So(err, ShouldBeNil)
		client := &http.Client{}
		baseURL := fmt.Sprintf("http://%s/post-collectd", listener.server.Addr)
		Convey("Should be able to recv collecd data", func() {
			var datapoints []*datapoint.Datapoint
			sendRecvData := func() {
				sendTo.Resize(10)
				req, err := http.NewRequest("POST", baseURL, strings.NewReader(testCollectdBody))
				So(err, ShouldBeNil)
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				So(err, ShouldBeNil)
				So(resp.StatusCode, ShouldEqual, http.StatusOK)
				datapoints = <-sendTo.PointsChan
				So(len(datapoints), ShouldEqual, 8)
				So(dptest.ExactlyOne(datapoints, "load.shortterm").Value.String(), ShouldEqual, "0.37")
			}
			Convey("with default dimensions", func() {
				baseURL = baseURL + "?" + sfxDimQueryParamPrefix + "default=dim"
				sendRecvData()
				expectedDims := map[string]string{"dsname": "value", "plugin": "dogstatsd", "env": "dev", "k1": "v1", "host": "some-host", "default": "dim"}
				So(dptest.ExactlyOne(datapoints, "gauge.page.loadtime").Dimensions, ShouldResemble, expectedDims)
			})
			Convey("with empty default dimensions", func() {
				baseURL = baseURL + "?" + sfxDimQueryParamPrefix + "default="
				So(dptest.ExactlyOne(listener.Datapoints(), "total_blank_dims").Value.String(), ShouldEqual, "0")
				sendRecvData()
				expectedDims := map[string]string{"dsname": "value", "plugin": "dogstatsd", "env": "dev", "k1": "v1", "host": "some-host"}
				So(dptest.ExactlyOne(datapoints, "gauge.page.loadtime").Dimensions, ShouldResemble, expectedDims)

				So(dptest.ExactlyOne(listener.Datapoints(), "total_blank_dims").Value.String(), ShouldEqual, "1")

			})
			Convey("without default dimensions", func() {
				sendRecvData()
				expectedDims := map[string]string{"dsname": "value", "plugin": "dogstatsd", "env": "dev", "k1": "v1", "host": "some-host"}
				So(dptest.ExactlyOne(datapoints, "gauge.page.loadtime").Dimensions, ShouldResemble, expectedDims)
			})
		})
		Convey("should return errors on invalid data", func() {
			req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/post-collectd", listener.server.Addr), bytes.NewBufferString("{invalid"))
			So(err, ShouldBeNil)
			req.Header.Set("Content-Type", "application/json")

			dps := listener.Datapoints()
			So(dptest.ExactlyOne(dps, "invalid_collectd_json").Value.String(), ShouldEqual, "0")
			resp, err := client.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusBadRequest)
			dps = listener.Datapoints()
			So(dptest.ExactlyOne(dps, "invalid_collectd_json").Value.String(), ShouldEqual, "1")
		})

		Reset(func() {
			So(listener.Close(), ShouldBeNil)
		})
	})
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
	sendTo.Resize(20)
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
		if len(item) != 1 {
			b.Fatalf("Saw more than one item: %d", len(item))
		}
		if len(sendTo.PointsChan) != 0 {
			b.Fatalf("Even more: %d?", len(sendTo.PointsChan))
		}
	}
	b.SetBytes(bytes)
}
