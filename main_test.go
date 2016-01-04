package main

import (
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/metricproxy/protocol/carbon"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const config1 = `
  {
    "LogFormat": "logfmt",
    "LogDir": "-",
    "ListenFrom":[
      {
      	"Type":"carbon",
      	"ListenAddr": "127.0.0.1:0"
	  }
    ],
    "ForwardTo":[
      {
      	"Type":"carbon",
      	"Host":"127.0.0.1",
      	"Name": "testForwardTo",
      	"Port": <<PORT>>
      }
    ]
  }
`

func TestProxy1(t *testing.T) {
	Convey("a setup carbon proxy", t, func() {
		fileObj, err := ioutil.TempFile("", "TestProxy")
		So(err, ShouldBeNil)
		filename := fileObj.Name()
		So(os.Remove(filename), ShouldBeNil)
		ctx, canceler := context.WithCancel(context.Background())
		sendTo := dptest.NewBasicSink()
		cconf := &carbon.ListenerConfig{}
		cl, err := carbon.NewListener(sendTo, cconf)
		So(err, ShouldBeNil)
		openPort := nettest.TCPPort(cl)
		proxyConf := strings.Replace(config1, "<<PORT>>", strconv.FormatInt(int64(openPort), 10), -1)
		So(ioutil.WriteFile(filename, []byte(proxyConf), os.FileMode(0666)), ShouldBeNil)
		p := proxy{
			flags: proxyFlags{
				configFileName: filename,
			},
			logger:          log.NewHierarchy(log.DefaultLogger),
			tk:              timekeeper.RealTime{},
			setupDoneSignal: make(chan struct{}),
		}
		mainDoneChan := make(chan struct{})
		go func() {
			p.main(ctx)
			close(mainDoneChan)
		}()
		<-p.setupDoneSignal
		listeningCarbonProxyPort := nettest.TCPPort(p.allListeners[0].(*carbon.Listener))

		Convey("should proxy a carbon point", func() {
			cf, err := carbon.NewForwarder("127.0.0.1", &carbon.ForwarderConfig{
				Port: pointer.Uint16(listeningCarbonProxyPort),
			})
			So(err, ShouldBeNil)
			dp := dptest.DP()
			dp.Dimensions = nil
			dp.Timestamp = dp.Timestamp.Round(time.Second)
			So(cf.AddDatapoints(ctx, []*datapoint.Datapoint{dp}), ShouldBeNil)
			seenDatapoint := sendTo.Next()
			So(dp.String(), ShouldEqual, seenDatapoint.String())
		})

		Reset(func() {
			canceler()
			<-mainDoneChan
			So(os.Remove(filename), ShouldBeNil)
			So(cl.Close(), ShouldBeNil)
		})
	})
}
