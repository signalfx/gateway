package protocol

import (
	"context"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/web"
	. "github.com/smartystreets/goconvey/convey"
	"net"
	"net/http"
	"testing"
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

func (c *listenerServer) Datapoints() []*datapoint.Datapoint {
	return c.HealthDatapoints()
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
