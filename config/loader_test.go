package config

import (
	"context"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/web"
	"github.com/signalfx/metricproxy/protocol/carbon"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

func getLoader() *Loader {
	ctx := context.Background()
	logger := log.Discard
	version := "123"
	debugContext := web.HeaderCtxFlag{}
	itemFlagger := dpsink.ItemFlagger{}
	l := NewLoader(ctx, logger, version, &debugContext, &itemFlagger, nil, nil)
	return l
}

type testCase struct {
	name       string
	forwardTo  *ForwardTo
	listenFrom *ListenFrom
	pass       bool
	reset      func() error
}

func TestForwarders(t *testing.T) {
	Convey("test some forwarders", t, func() {
		l := getLoader()
		tests := []testCase{
			{name: "should fail empty forwarders", forwardTo: &ForwardTo{}},
			{name: "should fail for unknown forwarders", forwardTo: &ForwardTo{Type: "unknown"}},
			{name: "carbon forwarder should require a host", forwardTo: &ForwardTo{Type: "carbon"}},
			{name: "should load CSV forwarder", forwardTo: &ForwardTo{Type: "csv", Filename: pointer.String("datapoints.csv")}, pass: true, reset: func() error { So(os.Remove("datapoints.csv"), ShouldBeNil); return nil }},
			{name: "signalfx forwarder should work", forwardTo: &ForwardTo{Type: "signalfx"}, pass: true},
		}
		for _, test := range tests {
			Convey(test.name, func() {
				f, err := l.Forwarder(test.forwardTo)
				if test.pass {
					So(err, ShouldBeNil)
					So(f.Close(), ShouldBeNil)
				} else {
					So(err, ShouldNotBeNil)
				}
				if test.reset != nil {
					test.reset()
				}
			})
		}
	})
}

func TestListeners(t *testing.T) {
	Convey("test some listeners", t, func() {
		l := getLoader()
		tests := []testCase{
			{name: "should fail empty listeners", listenFrom: &ListenFrom{}},
			{name: "should fail unknown listeners", listenFrom: &ListenFrom{Type: "unknown"}},
			{name: "carbon listener should require metric deconstructor", listenFrom: &ListenFrom{Type: "carbon", MetricDeconstructor: pointer.String("unknown")}},
			{name: "should load CollectdD listener", listenFrom: &ListenFrom{Type: "collectd", ListenAddr: pointer.String("127.0.0.1:0")}, pass: true},
			{name: "should load prometheus listener", listenFrom: &ListenFrom{Type: "prometheus", ListenAddr: pointer.String("127.0.0.1:0")}, pass: true},
			{name: "should load wavefront listener", listenFrom: &ListenFrom{Type: "wavefront", ListenAddr: pointer.String("127.0.0.1:0")}, pass: true},
			{name: "should load signalfx listener", listenFrom: &ListenFrom{Type: "signalfx", ListenAddr: pointer.String("127.0.0.1:0")}, pass: true},
			{name: "should load carbon listener", listenFrom: &ListenFrom{Type: "carbon", ListenAddr: pointer.String("127.0.0.1:0")}, pass: true},
		}
		for _, test := range tests {
			Convey(test.name, func() {
				f, err := l.Listener(nil, test.listenFrom)
				if test.pass {
					So(err, ShouldBeNil)
					So(f.Close(), ShouldBeNil)
				} else {
					So(err, ShouldNotBeNil)
				}
				if test.reset != nil {
					So(test.reset(), ShouldBeNil)
				}
			})
		}
	})
}

func TestConfigLoader(t *testing.T) {
	Convey("a setup loader", t, func() {
		l := getLoader()
		ctx := context.Background()
		sink := dptest.NewBasicSink()
		sink.Resize(1)
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
