package collectd

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/datapoint/dptest"
	"github.com/signalfx/golib/v3/pointer"
	"github.com/signalfx/golib/v3/web"
	. "github.com/smartystreets/goconvey/convey"
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

const largerCollectdBody = `[{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138708.700,"interval":10.000,"host":"mwp-signalbox1","plugin":"collectd","plugin_instance":"write_queue","type":"queue_length","type_instance":""},{"values":[0],"dstypes":["derive"],"dsnames":["value"],"time":1530138708.700,"interval":10.000,"host":"mwp-signalbox1","plugin":"collectd","plugin_instance":"write_queue","type":"derive","type_instance":"dropped"},{"values":[170],"dstypes":["gauge"],"dsnames":["value"],"time":1530138708.700,"interval":10.000,"host":"mwp-signalbox1","plugin":"collectd","plugin_instance":"cache","type":"cache_size","type_instance":""},{"values":[2449,1861],"dstypes":["derive","derive"],"dsnames":["rx","tx"],"time":1530138718.239,"interval":10.000,"host":"mwp-signalbox1","plugin":"interface","plugin_instance":"eth0","type":"if_packets","type_instance":""},{"values":[263379,191959],"dstypes":["derive","derive"],"dsnames":["rx","tx"],"time":1530138718.239,"interval":10.000,"host":"mwp-signalbox1","plugin":"interface","plugin_instance":"eth0","type":"if_octets","type_instance":""},{"values":[0,0],"dstypes":["derive","derive"],"dsnames":["rx","tx"],"time":1530138718.239,"interval":10.000,"host":"mwp-signalbox1","plugin":"interface","plugin_instance":"eth0","type":"if_errors","type_instance":""},{"values":[0,0],"dstypes":["derive","derive"],"dsnames":["rx","tx"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"interface","plugin_instance":"eth0","type":"if_dropped","type_instance":""},{"values":[121,128],"dstypes":["derive","derive"],"dsnames":["rx","tx"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"interface","plugin_instance":"eth1","type":"if_packets","type_instance":""},{"values":[11813,90934],"dstypes":["derive","derive"],"dsnames":["rx","tx"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"interface","plugin_instance":"eth1","type":"if_octets","type_instance":""},{"values":[0,0],"dstypes":["derive","derive"],"dsnames":["rx","tx"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"interface","plugin_instance":"eth1","type":"if_errors","type_instance":""},{"values":[0,0],"dstypes":["derive","derive"],"dsnames":["rx","tx"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"interface","plugin_instance":"eth1","type":"if_dropped","type_instance":""},{"values":[34593693696],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"root","type":"df_complex","type_instance":"free"},{"values":[1777573888],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"root","type":"df_complex","type_instance":"reserved"},{"values":[5869895680],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"root","type":"df_complex","type_instance":"used"},{"values":[4096],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"sys-fs-cgroup","type":"df_complex","type_instance":"free"},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"sys-fs-cgroup","type":"df_complex","type_instance":"reserved"},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"sys-fs-cgroup","type":"df_complex","type_instance":"used"},{"values":[4181184512],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"dev","type":"df_complex","type_instance":"free"},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"dev","type":"df_complex","type_instance":"reserved"},{"values":[12288],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"dev","type":"df_complex","type_instance":"used"},{"values":[393216],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"run","type":"df_complex","type_instance":"used"},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"run-lock","type":"df_complex","type_instance":"used"},{"values":[836911104],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"run","type":"df_complex","type_instance":"free"},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"run-shm","type":"df_complex","type_instance":"reserved"},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"run-shm","type":"df_complex","type_instance":"used"},{"values":[4186509312],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"run-shm","type":"df_complex","type_instance":"free"},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"run-user","type":"df_complex","type_instance":"reserved"},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"run-user","type":"df_complex","type_instance":"used"},{"values":[104857600],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"run-user","type":"df_complex","type_instance":"free"},{"values":[12324,1501],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"sda","type":"disk_ops","type_instance":""},{"values":[11,160],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"sda","type":"disk_time","type_instance":""},{"values":[76,3705],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"sda","type":"disk_merged","type_instance":""},{"values":[4740,19664],"dstypes":["derive","derive"],"dsnames":["io_time","weighted_io_time"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"sda","type":"disk_io_time","type_instance":""},{"values":[241755136,54362112],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"sda1","type":"disk_octets","type_instance":""},{"values":[12174,1469],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"sda1","type":"disk_ops","type_instance":""},{"values":[5242880],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"run-lock","type":"df_complex","type_instance":"free"},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"run","type":"df_complex","type_instance":"reserved"},{"values":[4712,19496],"dstypes":["derive","derive"],"dsnames":["io_time","weighted_io_time"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"sda1","type":"disk_io_time","type_instance":""},{"values":[1028096,0],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"dm-0","type":"disk_octets","type_instance":""},{"values":[251,0],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"dm-0","type":"disk_ops","type_instance":""},{"values":[1,0],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"dm-0","type":"disk_time","type_instance":""},{"values":[20,20],"dstypes":["derive","derive"],"dsnames":["io_time","weighted_io_time"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"dm-0","type":"disk_io_time","type_instance":""},{"values":[185658257408],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.241,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"opt","type":"df_complex","type_instance":"free"},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"run-lock","type":"df_complex","type_instance":"reserved"},{"values":[76,3705],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"sda1","type":"disk_merged","type_instance":""},{"values":[185658257408],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.241,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"vagrant","type":"df_complex","type_instance":"free"},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.241,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"vagrant","type":"df_complex","type_instance":"reserved"},{"values":[314409779200],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.241,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"opt","type":"df_complex","type_instance":"used"},{"values":[314409779200],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.241,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"vagrant","type":"df_complex","type_instance":"used"},{"values":[204914688],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"memory","plugin_instance":"","type":"memory","type_instance":"used"},{"values":[242369536,54362112],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"sda","type":"disk_octets","type_instance":""},{"values":[224272384],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"memory","plugin_instance":"","type":"memory","type_instance":"cached"},{"values":[11,161],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"disk","plugin_instance":"sda1","type":"disk_time","type_instance":""},{"values":[13275136],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"memory","plugin_instance":"","type":"memory","type_instance":"slab_unrecl"},{"values":[22933504],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"memory","plugin_instance":"","type":"memory","type_instance":"slab_recl"},{"values":[498],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"uptime","plugin_instance":"","type":"uptime","type_instance":""},{"values":[63439],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-sum","type":"cpu","type_instance":"idle","meta":{"aggregation:created":true}},{"values":[15859],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-average","type":"cpu","type_instance":"idle","meta":{"aggregation:created":true}},{"values":[0],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-sum","type":"cpu","type_instance":"steal","meta":{"aggregation:created":true}},{"values":[24846336],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"memory","plugin_instance":"","type":"memory","type_instance":"buffered"},{"values":[12],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-sum","type":"cpu","type_instance":"softirq","meta":{"aggregation:created":true}},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.241,"interval":10.000,"host":"mwp-signalbox1","plugin":"df","plugin_instance":"opt","type":"df_complex","type_instance":"reserved"},{"values":[7882776576],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.240,"interval":10.000,"host":"mwp-signalbox1","plugin":"memory","plugin_instance":"","type":"memory","type_instance":"free"},{"values":[0],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-average","type":"cpu","type_instance":"interrupt","meta":{"aggregation:created":true}},{"values":[0],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-sum","type":"cpu","type_instance":"nice","meta":{"aggregation:created":true}},{"values":[0,0.04,0.05],"dstypes":["gauge","gauge","gauge"],"dsnames":["shortterm","midterm","longterm"],"time":1530138718.241,"interval":10.000,"host":"mwp-signalbox1","plugin":"load","plugin_instance":"","type":"load","type_instance":""},{"values":[3],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-sum","type":"cpu","type_instance":"wait","meta":{"aggregation:created":true}},{"values":[0],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-average","type":"cpu","type_instance":"wait","meta":{"aggregation:created":true}},{"values":[0],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-average","type":"cpu","type_instance":"nice","meta":{"aggregation:created":true}},{"values":[23],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-average","type":"cpu","type_instance":"system","meta":{"aggregation:created":true}},{"values":[0],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-average","type":"cpu","type_instance":"steal","meta":{"aggregation:created":true}},{"values":[17],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-average","type":"cpu","type_instance":"user","meta":{"aggregation:created":true}},{"values":[121],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-sum","type":"cpu","type_instance":"system","meta":{"aggregation:created":true}},{"values":[70],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"protocols","plugin_instance":"Tcp","type":"protocol_counter","type_instance":"ActiveOpens"},{"values":[4],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"protocols","plugin_instance":"Tcp","type":"protocol_counter","type_instance":"PassiveOpens"},{"values":[3],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"protocols","plugin_instance":"Tcp","type":"protocol_counter","type_instance":"CurrEstab"},{"values":[1872],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"protocols","plugin_instance":"Tcp","type":"protocol_counter","type_instance":"OutSegs"},{"values":[0],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"protocols","plugin_instance":"Tcp","type":"protocol_counter","type_instance":"RetransSegs"},{"values":[11],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.243,"interval":10.000,"host":"mwp-signalbox1","plugin":"protocols","plugin_instance":"TcpExt","type":"protocol_counter","type_instance":"DelayedACKs"},{"values":[0],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-sum","type":"cpu","type_instance":"interrupt","meta":{"aggregation:created":true}},{"values":[7632],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.243,"interval":10.000,"host":"mwp-signalbox1","plugin":"vmem","plugin_instance":"","type":"vmpage_number","type_instance":"mapped"},{"values":[1116996,1339],"dstypes":["derive","derive"],"dsnames":["minflt","majflt"],"time":1530138718.244,"interval":10.000,"host":"mwp-signalbox1","plugin":"vmem","plugin_instance":"","type":"vmpage_faults","type_instance":""},{"values":[250185,53096],"dstypes":["derive","derive"],"dsnames":["in","out"],"time":1530138718.244,"interval":10.000,"host":"mwp-signalbox1","plugin":"vmem","plugin_instance":"","type":"vmpage_io","type_instance":"memory"},{"values":[0,0],"dstypes":["derive","derive"],"dsnames":["in","out"],"time":1530138718.244,"interval":10.000,"host":"mwp-signalbox1","plugin":"vmem","plugin_instance":"","type":"vmpage_io","type_instance":"swap"},{"values":[8],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"protocols","plugin_instance":"Icmp","type":"protocol_counter","type_instance":"InDestUnreachs"},{"values":[1924506],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.243,"interval":10.000,"host":"mwp-signalbox1","plugin":"vmem","plugin_instance":"","type":"vmpage_number","type_instance":"free_pages"},{"values":[72],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-sum","type":"cpu","type_instance":"user","meta":{"aggregation:created":true}},{"values":[2],"dstypes":["derive"],"dsnames":["value"],"time":1530138718.242,"interval":10.000,"host":"mwp-signalbox1","plugin":"aggregation","plugin_instance":"cpu-average","type":"cpu","type_instance":"softirq","meta":{"aggregation:created":true}},{"values":[0.252016129032258],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"utilization","type":"cpu.utilization","type_instance":"","meta":{"0":true}},{"values":[0.0469621367772234],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"run","type":"disk.utilization","type_instance":"","meta":{"0":true}},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"sys-fs-cgroup","type":"disk.utilization","type_instance":"","meta":{"0":true}},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"run-user","type":"disk.utilization","type_instance":"","meta":{"0":true}},{"values":[62.8734004541993],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"vagrant","type":"disk.utilization","type_instance":"","meta":{"0":true}},{"values":[0.000293887147335423],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"dev","type":"disk.utilization","type_instance":"","meta":{"0":true}},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"run-shm","type":"disk.utilization","type_instance":"","meta":{"0":true}},{"values":[62.8734004541993],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"opt","type":"disk.utilization","type_instance":"","meta":{"0":true}},{"values":[628330.945968628],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.486,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"","type":"gauge","type_instance":"sf.host-response.max","meta":{"0":true}},{"values":[46],"dstypes":["counter"],"dsnames":["value"],"time":1530138718.486,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"","type":"counter","type_instance":"sf.host-response.errors","meta":{"0":true}},{"values":[223603],"dstypes":["counter"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"summation","type":"network.total","type_instance":"","meta":{"0":true}},{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"run-lock","type":"disk.utilization","type_instance":"","meta":{"0":true}},{"values":[170],"dstypes":["counter"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"summation","type":"disk_ops.total","type_instance":"","meta":{"0":true}},{"values":[14.5066114265226],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"root","type":"disk.utilization","type_instance":"","meta":{"0":true}},{"values":[60.4515597956281],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"utilization","type":"disk.summary_utilization","type_instance":"","meta":{"0":true}},{"values":[190.001095056534],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.486,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"[metadata=0.0.29,collectd=5.8.0.sfx0]","type":"gauge","type_instance":"sf.host-plugin_uptime[linux=Ubuntu 14.04.5 LTS,release=3.13.0-53-generic,version=#89-Ubuntu SMP Wed May 20 10:34:39 UTC 2015]","meta":{"0":true}},{"values":[2.4473215360186],"dstypes":["gauge"],"dsnames":["value"],"time":1530138718.200,"interval":10.000,"host":"mwp-signalbox1","plugin":"signalfx-metadata","plugin_instance":"utilization","type":"memory.utilization","type_instance":"","meta":{"0":true}}]`

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
		callCount := int64(0)
		conf := &ListenerConfig{
			ListenAddr:   pointer.String("127.0.0.1:0"),
			DebugContext: debugContext,
			HTTPChain: func(ctx context.Context, rw http.ResponseWriter, r *http.Request, next web.ContextHandler) {
				atomic.AddInt64(&callCount, 1)
				next.ServeHTTPC(ctx, rw, r)
			},
		}
		sendTo := dptest.NewBasicSink()

		listener, err := NewListener(sendTo, conf)
		So(err, ShouldBeNil)
		client := &http.Client{}
		baseURL := fmt.Sprintf("http://%s/post-collectd", listener.server.Addr)
		Convey("Should expose health check", func() {
			req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/healthz", listener.server.Addr), nil)
			So(err, ShouldBeNil)
			resp, err := client.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
			So(atomic.LoadInt64(&callCount), ShouldEqual, 1)
		})
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
		body := strings.NewReader(largerCollectdBody)
		req, _ := http.NewRequest("GET", "http://example.com/collectd", body)
		req.Header.Add("Content-type", "application/json")
		b.StartTimer()
		c.ServeHTTPC(ctx, writter, req)
		b.StopTimer()
		bytes += int64(len(largerCollectdBody))
		item := <-sendTo.PointsChan
		if len(item) != 132 {
			b.Fatalf("Saw more than one item: %d", len(item))
		}
		if len(sendTo.PointsChan) != 0 {
			b.Fatalf("Even more: %d?", len(sendTo.PointsChan))
		}
	}
	b.SetBytes(bytes)
}
