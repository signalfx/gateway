package wavefront

import (
	"errors"
	"fmt"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/pointer"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestFromWavefrontDatapoint(t *testing.T) {
	for _, tt := range []struct {
		doit               bool
		desc               string
		line               string
		expectedMetric     string
		expectedDimensions map[string]string
		expectedValue      string
	}{
		{
			doit:           true,
			desc:           "failure to parse",
			line:           "ima.metric.name",
			expectedMetric: "", // code for nil
		},
		{
			doit:           true,
			desc:           "failure to parse no timestamp",
			line:           "foo bar baz",
			expectedMetric: "", // code for nil
		},
		{
			doit:               true,
			desc:               "simple no dimensions, test integer",
			line:               "ima.metric.name 566 1511370774",
			expectedMetric:     "ima.metric.name",
			expectedDimensions: map[string]string{},
			expectedValue:      "566",
		},
		{
			doit:               false,
			desc:               "don't do it, also test float and no timestamp",
			line:               "ima.metric.name[foo=bar] 566.005707 source=source",
			expectedMetric:     "ima.metric.name[foo=bar]",
			expectedDimensions: map[string]string{"source": "source"},
			expectedValue:      "566.005707",
		},
		{
			doit:               true,
			desc:               "complex",
			line:               "collectd.signalfx-metadata.[metadata=0.0.29,collectd=5.7.2.sfx0].gauge.sf.host-plugin_uptime[linux=Ubuntu_14.04.5_LTS,release=3.13.0-53-generic,version=_89-Ubuntu_SMP_Wed_May_20_10:34:39_UTC_2015] 566.005707 1511370774 host=mwp-signalbox  use=prod dc=mwp",
			expectedMetric:     "collectd.signalfx-metadata.gauge.sf.host-plugin_uptime",
			expectedDimensions: map[string]string{"collectd": "5.7.2.sfx0", "linux": "Ubuntu_14.04.5_LTS", "metadata": "0.0.29", "release": "3.13.0-53-generic", "version": "_89-Ubuntu_SMP_Wed_May_20_10:34:39_UTC_2015", "host": "mwp-signalbox", "use": "prod", "dc": "mwp"},
			expectedValue:      "566.005707",
		},
	} {
		Convey(tt.desc, t, func() {
			listener := &Listener{}
			listener.extractCollectdDimensions = tt.doit
			dp := listener.fromWavefrontDatapoint(tt.line)
			if tt.expectedMetric != "" {
				So(dp, ShouldNotBeNil)
				So(dp.Metric, ShouldEqual, tt.expectedMetric)
				So(dp.Dimensions, ShouldResemble, tt.expectedDimensions)
				So(dp.Value.String(), ShouldEqual, tt.expectedValue)
			} else {
				So(dp, ShouldBeNil)
			}
		})
	}
}

var errDeadline = errors.New("nope")

type undeadlineable struct {
	net.Conn
}

func (u *undeadlineable) SetDeadline(t time.Time) error {
	return errDeadline
}

func TestWavefrontListenerBadAddr(t *testing.T) {
	Convey("bad listener ports shouldn't be able to accept", t, func() {
		listenFrom := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:90090999r"),
		}
		sendTo := dptest.NewBasicSink()
		_, err := NewListener(sendTo, listenFrom)
		So(err, ShouldNotBeNil)
	})
	Convey("bad udp listener ports shouldn't be able to accept", t, func() {
		listenFrom := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:90090999r"),
		}
		sendTo := dptest.NewBasicSink()
		_, err := NewListener(sendTo, listenFrom)
		So(err, ShouldNotBeNil)
	})
}

func TestWavefrontListenerNormalTCP(t *testing.T) {
	Convey("A normally setup listener", t, func() {
		listenFrom := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:0"),
		}
		sendTo := dptest.NewBasicSink()
		listener, err := NewListener(sendTo, listenFrom)
		So(err, ShouldBeNil)
		Convey("should eventually time out idle connections", func() {
			listenFrom.ConnectionTimeout = pointer.Duration(time.Millisecond)
			listenFrom.ServerAcceptDeadline = pointer.Duration(time.Millisecond)
			So(listener.Close(), ShouldBeNil)
			listener, err = NewListener(sendTo, listenFrom)
			So(err, ShouldBeNil)

			connAddr := fmt.Sprintf("127.0.0.1:%d", nettest.TCPPort(listener))
			s, err := net.Dial("tcp", connAddr)
			So(err, ShouldBeNil)
			// Wait for the idle timeout
			for atomic.LoadInt64(&listener.stats.idleTimeouts) == 0 {
				time.Sleep(time.Millisecond)
			}
			So(s.Close(), ShouldBeNil)
			dps := listener.Datapoints()
			So(dptest.ExactlyOne(dps, "idle_timeouts").Value.String(), ShouldEqual, "1")

			Convey("and idle in its its own listen loop", func() {
				for atomic.LoadInt64(&listener.stats.retriedListenErrors) == 0 {
					time.Sleep(time.Millisecond)
				}
				dps := listener.Datapoints()
				So(dptest.ExactlyOne(dps, "retry_listen_errors").Value.String(), ShouldNotEqual, "0")
			})
		})
		Convey("should error invalid lines", func() {
			dps := listener.Datapoints()
			So(dptest.ExactlyOne(dps, "invalid_datapoints").Value.String(), ShouldEqual, "0")

			connAddr := fmt.Sprintf("127.0.0.1:%d", nettest.TCPPort(listener))
			s, err := net.Dial("tcp", connAddr)
			So(err, ShouldBeNil)
			_, err = io.WriteString(s, "hello world\n")
			So(err, ShouldBeNil)
			So(s.Close(), ShouldBeNil)

			for atomic.LoadInt64(&listener.stats.invalidDatapoints) == 0 {
				time.Sleep(time.Millisecond)
			}

			dps = listener.Datapoints()
			So(dptest.ExactlyOne(dps, "invalid_datapoints").Value.String(), ShouldEqual, "1")
		})
		Convey("should not error valid lines", func() {
			sendTo.Resize(10)
			connAddr := fmt.Sprintf("127.0.0.1:%d", nettest.TCPPort(listener))
			s, err := net.Dial("tcp", connAddr)
			So(err, ShouldBeNil)
			_, err = io.WriteString(s, "ima.metric.name 566 1511370774\n")
			So(err, ShouldBeNil)
			So(s.Close(), ShouldBeNil)

			for atomic.LoadInt64(&listener.stats.totalDatapoints) == 0 {
				time.Sleep(time.Millisecond)
			}

			dps := <-sendTo.PointsChan
			So(len(dps), ShouldEqual, 1)
			So(dps[0].Metric, ShouldEqual, "ima.metric.name")
		})
		Reset(func() {
			So(listener.Close(), ShouldBeNil)
		})
	})
}
