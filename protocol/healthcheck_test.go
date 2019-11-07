package protocol

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/gorilla/mux"
	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/datapoint/dptest"
	"github.com/signalfx/golib/v3/log"
	"github.com/signalfx/golib/v3/nettest"
	"github.com/signalfx/golib/v3/pointer"
	"github.com/signalfx/golib/v3/web"
	. "github.com/smartystreets/goconvey/convey"
)

type listenerServer struct {
	CloseableHealthCheck
	server   http.Server
	listener net.Listener
}

func (c *listenerServer) Close() error {
	err := c.listener.Close()
	return err
}

func (c *listenerServer) DebugDatapoints() []*datapoint.Datapoint {
	return c.HealthDatapoints()
}

func (c *listenerServer) DefaultDatapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{}
}

func (c *listenerServer) Datapoints() []*datapoint.Datapoint {
	return append(c.DebugDatapoints(), c.DefaultDatapoints()...)
}

var _ Listener = &listenerServer{}

func grabHealthCheck(t *testing.T, baseURI string, status int) {
	client := http.Client{}
	req, err := http.NewRequest("GET", baseURI+"/healthz", nil)
	So(err, ShouldBeNil)
	resp, err := client.Do(req)
	So(err, ShouldBeNil)
	So(resp.StatusCode, ShouldEqual, status)
}

func TestHealthCheck(t *testing.T) {
	Convey("with a listener we setup the health check", t, func() {
		listenAddr := "127.0.0.1:0"
		sendTo := dptest.NewBasicSink()
		sendTo.Resize(1)
		listener, err := net.Listen("tcp", listenAddr)
		So(err, ShouldBeNil)
		r := mux.NewRouter()
		fullHandler := web.NewHandler(context.Background(), web.FromHTTP(r))
		listenServer := &listenerServer{
			listener: listener,
			server: http.Server{
				Handler: fullHandler,
				Addr:    listener.Addr().String(),
			},
		}
		baseURI := fmt.Sprintf("http://127.0.0.1:%d", nettest.TCPPort(listener))
		So(listener.Addr(), ShouldNotBeNil)
		listenServer.SetupHealthCheck(pointer.String("/healthz"), r, log.Discard)
		go func() {
			err := listenServer.server.Serve(listener)
			log.IfErr(log.Discard, err)
		}()
		Convey("Should expose health check", func() {
			grabHealthCheck(t, baseURI, http.StatusOK)
			dp := listenServer.HealthDatapoints()
			So(len(dp), ShouldEqual, 1)
			fmt.Println(dp)
			So(dp[0].Value, ShouldEqual, 1)
			Convey("and we close the health check", func() {
				listenServer.CloseHealthCheck()
				grabHealthCheck(t, baseURI, http.StatusNotFound)
				dp := listenServer.HealthDatapoints()
				So(len(dp), ShouldEqual, 1)
				fmt.Println(dp)
				So(dp[0].Value, ShouldEqual, 2)
			})
		})
	})
}
