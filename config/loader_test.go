package config

import (
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/web"
	"github.com/signalfx/metricproxy/protocol/carbon"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
	"os"
	"testing"
)

func TestConfigLoader(t *testing.T) {
	Convey("a setup loader", t, func() {
		ctx := context.Background()
		logger := log.Discard
		version := "123"
		sink := dptest.NewBasicSink()
		sink.Resize(1)

		debugContext := web.HeaderCtxFlag{}
		itemFlagger := dpsink.ItemFlagger{}

		l := NewLoader(ctx, logger, version, &debugContext, &itemFlagger, nil, nil)
		Convey("should fail empty forwarders", func() {
			_, err := l.Forwarder(&ForwardTo{})
			So(err, ShouldNotBeNil)
		})
		Convey("should fail empty listeners", func() {
			_, err := l.Listener(nil, &ListenFrom{})
			So(err, ShouldNotBeNil)
		})
		Convey("should fail unknown forwarders", func() {
			_, err := l.Forwarder(&ForwardTo{Type: "unknown"})
			So(err, ShouldNotBeNil)
		})
		Convey("should fail unknown listeners", func() {
			_, err := l.Listener(nil, &ListenFrom{Type: "unknown"})
			So(err, ShouldNotBeNil)
		})
		Convey("carbon forwarder should require a host", func() {
			_, err := l.Forwarder(&ForwardTo{Type: "carbon"})
			So(err, ShouldNotBeNil)
		})
		Convey("carbon listener should require metricdeconstructor to load", func() {
			_, err := l.Listener(nil, &ListenFrom{Type: "carbon", MetricDeconstructor: pointer.String("unknown")})
			So(err, ShouldNotBeNil)
		})

		Convey("should load CSV forwarder", func() {
			f, err := l.Forwarder(&ForwardTo{Type: "csv", Filename: pointer.String("datapoints.csv")})
			So(err, ShouldBeNil)
			So(f.Close(), ShouldBeNil)
			So(os.Remove("datapoints.csv"), ShouldBeNil)
		})

		Convey("should load CollectD listener", func() {
			f, err := l.Listener(nil, &ListenFrom{Type: "collectd", ListenAddr: pointer.String("127.0.0.1:0")})
			So(err, ShouldBeNil)
			So(f.Close(), ShouldBeNil)
		})
		Convey("should load prometheus listener", func() {
			f, err := l.Listener(nil, &ListenFrom{Type: "prometheus", ListenAddr: pointer.String("127.0.0.1:0")})
			So(err, ShouldBeNil)
			So(f.Close(), ShouldBeNil)
		})
		Convey("should load signalfx listener", func() {
			f, err := l.Listener(nil, &ListenFrom{Type: "signalfx", ListenAddr: pointer.String("127.0.0.1:0")})
			So(err, ShouldBeNil)
			So(f.Close(), ShouldBeNil)
		})
		Convey("should load signalfx forwarder", func() {
			f, err := l.Forwarder(&ForwardTo{Type: "signalfx"})
			So(err, ShouldBeNil)
			So(f.Close(), ShouldBeNil)
		})
		Convey("should load carbon listener without options", func() {
			f, err := l.Listener(nil, &ListenFrom{Type: "carbon", ListenAddr: pointer.String("127.0.0.1:0")})
			So(err, ShouldBeNil)
			So(f.Close(), ShouldBeNil)
		})
		Convey("should load carbon listener", func() {
			f, err := l.Listener(sink, &ListenFrom{Dimensions: map[string]string{"name": "john"}, Type: "carbon", MetricDeconstructor: pointer.String(""), MetricDeconstructorOptions: pointer.String(""), ListenAddr: pointer.String("127.0.0.1:0")})
			So(err, ShouldBeNil)
			carbonPort := nettest.TCPPort(f.(*carbon.Listener))
			Convey("should load carbon forwarder", func() {
				f, err := l.Forwarder(&ForwardTo{Type: "carbon", Host: pointer.String("127.0.0.1"), Port: pointer.Uint16(carbonPort)})
				So(err, ShouldBeNil)

				Convey("should add dimensions to the listener", func() {
					dp := dptest.DP()
					dp.Dimensions = nil
					So(f.AddDatapoints(ctx, []*datapoint.Datapoint{dp}), ShouldBeNil)
					dpOut := sink.Next()
					So(dpOut.Dimensions, ShouldResemble, map[string]string{"name": "john"})
				})
				Reset(func() {
					So(f.Close(), ShouldBeNil)
				})
			})
			Reset(func() {
				So(f.Close(), ShouldBeNil)
			})
		})
	})
}
