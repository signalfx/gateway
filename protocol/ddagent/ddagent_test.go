package ddagent

import "testing"
import (
	"encoding/json"

	"errors"

	"github.com/signalfx/golib/datapoint"
	. "github.com/smartystreets/goconvey/convey"
)

var examplePost = `
{
    "agentVersion": "5.6.0",
    "agent_checks": [
        [
            "ntp",
            "ntp",
            0,
            "OK",
            "",
            {}
        ],
        [
            "disk",
            "disk",
            0,
            "OK",
            "",
            {}
        ],
        [
            "network",
            "system",
            0,
            "OK",
            "",
            {}
        ]
    ],
    "apiKey": "abcd",
    "collection_timestamp": 1443230370.495594,
    "cpuIdle": 97.0,
    "cpuStolen": 0,
    "cpuSystem": 1.0,
    "cpuUser": 2.0,
    "cpuWait": 0,
    "events": {
        "System": [
            {
                "api_key": "abcd",
                "event_type": "Agent Startup",
                "host": "Jacks-MacBook-Pro.local",
                "msg_text": "Version 5.6.0",
                "timestamp": 1443230381.84419
            }
        ]
    },
    "external_host_tags": {},
    "host-tags": {},
    "internalHostname": "Jacks-MacBook-Pro.local",
    "ioStats": {
        "disk0": {
            "system.io.bytes_per_s": 0.0
        },
        "disk2": {
            "system.io.bytes_per_s": 0.0
        }
    },
    "memBuffers": null,
    "memCached": null,
    "memPhysFree": "2891",
    "memPhysPctUsable": null,
    "memPhysTotal": null,
    "memPhysUsable": null,
    "memPhysUsed": "13",
    "memShared": null,
    "memSwapFree": "1711.00",
    "memSwapPctFree": null,
    "memSwapTotal": null,
    "memSwapUsed": "337.00",
    "meta": {
        "hostname": "Jacks-MacBook-Pro.local",
        "socket-fqdn": "Jacks-MacBook-Pro.local",
        "socket-hostname": "Jacks-MacBook-Pro.local"
    },
    "metrics": [
        [
            "go_expvar.memstats.heap_released",
            1443490939,
            0,
            {
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge",
                "tags": ["expvar_url:http://localhost:6060/debug/vars"]
            }
        ],
        [
            "ntp.offset",
            1443230376,
            0.08751559257507324,
            {
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.fs.inodes.in_use",
            1443230376,
            1.0,
            {
                "device_name": "devfs",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.disk.used",
            1443230376,
            125322032.0,
            {
                "device_name": "/dev/disk1",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.fs.inodes.used",
            1443230376,
            31394506,
            {
                "device_name": "/dev/disk1",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.fs.inodes.total",
            1443230376,
            121842174,
            {
                "device_name": "/dev/disk1",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.fs.inodes.total",
            1443230376,
            1152,
            {
                "device_name": "devfs",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.fs.inodes.used",
            1443230376,
            1152,
            {
                "device_name": "devfs",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.disk.in_use",
            1443230376,
            1.0,
            {
                "device_name": "devfs",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.fs.inodes.free",
            1443230376,
            0,
            {
                "device_name": "devfs",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.disk.in_use",
            1443230376,
            0.2572752280342908,
            {
                "device_name": "/dev/disk1",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.disk.free",
            1443230376,
            361790672.0,
            {
                "device_name": "/dev/disk1",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.disk.total",
            1443230376,
            487368704.0,
            {
                "device_name": "/dev/disk1",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.disk.total",
            1443230376,
            333.0,
            {
                "device_name": "devfs",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.disk.free",
            1443230376,
            0.0,
            {
                "device_name": "devfs",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.fs.inodes.free",
            1443230376,
            90447668,
            {
                "device_name": "/dev/disk1",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.fs.inodes.in_use",
            1443230376,
            0.2576653466475409,
            {
                "device_name": "/dev/disk1",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ],
        [
            "system.disk.used",
            1443230376,
            333.0,
            {
                "device_name": "devfs",
                "hostname": "Jacks-MacBook-Pro.local",
                "type": "gauge"
            }
        ]
    ],
    "os": "mac",
    "processes": {
        "apiKey": "abcd",
        "host": "Jacks-MacBook-Pro.local",
        "processes": [
            [
                "_windowserver",
                "180",
                "13.2",
                "0.9",
                "5216348",
                "157020",
                "??",
                "Ss",
                "17Sep15",
                "246:20.97",
                "/System/Library/Frameworks/ApplicationServices.framework/Frameworks/CoreGraphics.framework/Resources/WindowServer -daemon"
            ]
        ]
    },
    "python": "2.7.8 (default, Sep  9 2014, 11:09:59) \n[GCC 4.2.1 Compatible Apple LLVM 5.1 (clang-503.0.40)]",
    "resources": {},
    "service_checks": [
        {
            "check": "ntp.in_sync",
            "host_name": "Jacks-MacBook-Pro.local",
            "id": 1,
            "message": null,
            "status": 0,
            "tags": null,
            "timestamp": 1443230376.107583
        },
        {
            "check": "datadog.agent.check_status",
            "host_name": "Jacks-MacBook-Pro.local",
            "id": 2,
            "message": null,
            "status": 0,
            "tags": [
                "check:ntp"
            ],
            "timestamp": 1443230376.073971
        },
        {
            "check": "datadog.agent.check_status",
            "host_name": "Jacks-MacBook-Pro.local",
            "id": 3,
            "message": null,
            "status": 0,
            "tags": [
                "check:disk"
            ],
            "timestamp": 1443230376.075784
        },
        {
            "check": "datadog.agent.check_status",
            "host_name": "Jacks-MacBook-Pro.local",
            "id": 4,
            "message": null,
            "status": 0,
            "tags": [
                "check:network"
            ],
            "timestamp": 1443230381.843828
        },
        {
            "check": "datadog.agent.up",
            "host_name": "Jacks-MacBook-Pro.local",
            "id": 5,
            "message": null,
            "status": 0,
            "tags": null,
            "timestamp": 1443230381.84416
        }
    ],
    "system.load.1": 2.29,
    "system.load.15": 2.02,
    "system.load.5": 2.15,
    "system.load.norm.1": 0.28625,
    "system.load.norm.15": 0.2525,
    "system.load.norm.5": 0.26875,
    "systemStats": {
        "cpuCores": 8,
        "macV": [
            "10.10.5",
            [
                "",
                "",
                ""
            ],
            "x86_64"
        ],
        "machine": "x86_64",
        "platform": "darwin",
        "processor": "i386",
        "pythonV": "2.7.8"
    },
    "uuid": "d14f939e04bc55d0a75086f990820492"
}
`

func TestMetricDecoding(t *testing.T) {
	Convey("example post should decode", t, func() {
		g := ddAgentIntakePayload{}
		So(json.Unmarshal([]byte(examplePost), &g), ShouldBeNil)
		Convey("and should contain 18 metrics", func() {
			So(len(g.Metrics), ShouldEqual, 18)
		})
		Convey("and second metric should have correct values", func() {
			//			"ntp.offset",
			//			1443230376,
			//			0.08751559257507324,
			//			{
			//				"hostname": "Jacks-MacBook-Pro.local",
			//			"type": "gauge"
			//			}
			So(g.Metrics[1].metric, ShouldEqual, "ntp.offset")
			So(g.Metrics[1].timestamp, ShouldEqual, 1443230376)
			So(*g.Metrics[1].attributes.Hostname, ShouldEqual, "Jacks-MacBook-Pro.local")
			So(*g.Metrics[1].attributes.Type, ShouldEqual, "gauge")
			So(g.Metrics[1].attributes.Tags, ShouldBeNil)
			val, _ := g.Metrics[1].value.Float64()
			So(val, ShouldEqual, 0.08751559257507324)
		})
		Convey("and first metric should create the correct datapoint", func() {
			dp, err := g.Metrics[0].Datapoint()
			So(err, ShouldBeNil)
			So(dp.Timestamp.Unix(), ShouldEqual, 1443490939)
		})
		Convey("and first metric should have tags", func() {
			So(g.Metrics[0].attributes.Tags, ShouldResemble, []string{"expvar_url:http://localhost:6060/debug/vars"})
			Convey("and the correct datapoint type", func() {
				mtype, err := g.Metrics[0].attributes.MetricType()
				So(err, ShouldBeNil)
				So(mtype, ShouldEqual, datapoint.Gauge)
			})
			Convey("and the correct dimensions", func() {
				So(g.Metrics[0].attributes.IntoDimensions(), ShouldResemble, map[string]string{"expvar_url": "http://localhost:6060/debug/vars", "host": "Jacks-MacBook-Pro.local"})
			})
		})
	})
}

func TestDdAgentFormattedMetricDatapointErrors(t *testing.T) {
	errShouldEqual := func(b string, shouldEqual error) {
		d := ddAgentFormattedMetric{}
		So(json.Unmarshal([]byte(b), &d), ShouldBeNil)
		_, err := d.Datapoint()
		So(err.Error(), ShouldEqual, shouldEqual.Error())
	}
	Convey("empty metric name should be an invalid datapoint", t, func() {
		errShouldEqual(`
        [
            "",
            1443230376,
            0.08751559257507324,
            {
            }
        ]
        `, errUnsupportedMetricFormat)
	})
	Convey("Invalid numbers should be an invalid datapoint", t, func() {
		errShouldEqual(`
        [
            "test",
            1443230376,
            "abcd",
            {
            }
        ]
        `, errUnsupportedValue)
	})
	Convey("Invalid metric type should be an invalid datapoint", t, func() {
		errShouldEqual(`
        [
            "test",
            1443230376,
            1,
            {
                "type": "__UNKNOWN__"
            }
        ]
        `, errors.New("unknown metric type: __UNKNOWN__"))
	})
}

func TestDdAgentFormattedMetricJSONParsing(t *testing.T) {
	Convey("Invalid JSON shouldn't parse", t, func() {
		b := `__INVALID__`
		d := ddAgentFormattedMetric{}
		So(d.UnmarshalJSON([]byte(b)), ShouldNotBeNil)
	})
	Convey("bad []len shouldn't parse", t, func() {
		b := `[123]`
		d := ddAgentFormattedMetric{}
		So(d.UnmarshalJSON([]byte(b)), ShouldEqual, errUnexpectedMetricLen)
	})
	Convey("bad metric name shouldn't parse", t, func() {
		b := `[123, 123, 123, {}]`
		d := ddAgentFormattedMetric{}
		So(d.UnmarshalJSON([]byte(b)), ShouldNotBeNil)
	})
	Convey("bad timestamp shouldn't parse", t, func() {
		b := `["asfe", "asdf", 123, {}]`
		d := ddAgentFormattedMetric{}
		So(d.UnmarshalJSON([]byte(b)), ShouldNotBeNil)
	})
	Convey("bad value shouldn't parse", t, func() {
		b := `["asfe", 123, [], {}]`
		d := ddAgentFormattedMetric{}
		So(d.UnmarshalJSON([]byte(b)), ShouldNotBeNil)
	})
	Convey("bad attributes shouldn't parse", t, func() {
		b := `["asfe", 123, 123, "asd"]`
		d := ddAgentFormattedMetric{}
		So(d.UnmarshalJSON([]byte(b)), ShouldNotBeNil)
	})
}

func TestDdAgentFormattedMetricDatapointNormal(t *testing.T) {
	Convey("floats should parse", t, func() {
		b := `
        [
            "test",
            1443230376,
            0.08751559257507324,
            {
            }
        ]
        `
		d := ddAgentFormattedMetric{}
		So(json.Unmarshal([]byte(b), &d), ShouldBeNil)
		dp, err := d.Datapoint()
		So(err, ShouldBeNil)
		So(dp.Value.(datapoint.FloatValue).Float(), ShouldEqual, 0.08751559257507324)
	})
	Convey("ints should parse", t, func() {
		b := `
        [
            "test",
            1443230376,
            123,
            {
            "device_name": "abc"
            }
        ]
        `
		d := ddAgentFormattedMetric{}
		So(json.Unmarshal([]byte(b), &d), ShouldBeNil)
		dp, err := d.Datapoint()
		So(err, ShouldBeNil)
		So(dp.Value.(datapoint.IntValue).Int(), ShouldEqual, 123)
		Convey("and device dimension should propogate", func() {
			So(dp.Dimensions["device"], ShouldEqual, "abc")
		})
	})
}
