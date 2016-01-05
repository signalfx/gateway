package main

import (
	"bytes"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/metricproxy/protocol/carbon"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"github.com/signalfx/metricproxy/config"
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

const invalidForwarderConfig = `
  {
    "ListenFrom":[
    ],
    "ForwardTo":[
      {
      	"Type":"unkndfdown"
      }
    ]
  }
`

const invalidListenerConfig = `
  {
    "ListenFrom":[
    	{
    		"Type":"unknown"
		}
    ],
    "ForwardTo":[
    ]
  }
`

func TestFailingConfigs(t *testing.T) {
	Convey("bad configs should fail to load", t, func() {
		invalidConfigs := []string{
			invalidForwarderConfig,
			"__INVALID__JSON__",
			invalidListenerConfig,
		}
		for _, c := range invalidConfigs {
			logBuf := &bytes.Buffer{}
			fileObj, err := ioutil.TempFile("", "TestProxy")
			So(err, ShouldBeNil)
			filename := fileObj.Name()
			So(os.Remove(filename), ShouldBeNil)
			ctx := context.Background()
			So(ioutil.WriteFile(filename, []byte(c), os.FileMode(0666)), ShouldBeNil)
			p := proxy{
				flags: proxyFlags{
					configFileName: filename,
				},
				logger:          log.NewHierarchy(log.NewLogfmtLogger(io.MultiWriter(logBuf, os.Stderr), log.Panic)),
				tk:              timekeeper.RealTime{},
			}
			So(p.main(ctx), ShouldNotBeNil)
		}
	})
}

func TestProxy1(t *testing.T) {
	Convey("a setup carbon proxy", t, func() {
		logBuf := &bytes.Buffer{}
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
			logger:          log.NewHierarchy(log.NewLogfmtLogger(io.MultiWriter(logBuf, os.Stderr), log.Panic)),
			tk:              timekeeper.RealTime{},
			setupDoneSignal: make(chan struct{}),
		}
		mainDoneChan := make(chan struct{})
		go func() {
			p.main(ctx)
			close(mainDoneChan)
		}()
		<-p.setupDoneSignal
		listeningCarbonProxyPort := nettest.TCPPort(p.listeners[0].(*carbon.Listener))

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

		Convey("getLogOutput should work correctly", func() {
			So(p.getLogOutput(&config.ProxyConfig{
				LogDir: pointer.String("-"),
			}), ShouldEqual, os.Stdout)
			So(p.getLogOutput(&config.ProxyConfig{
				LogDir: pointer.String(""),
				LogMaxSize: pointer.Int(0),
				LogMaxBackups: pointer.Int(0),
			}), ShouldNotEqual, os.Stdout)
			l := p.getLogger(&config.ProxyConfig{
				LogDir: pointer.String("-"),
				LogFormat: pointer.String("json"),
			})
			So(l.(*log.ErrorLogLogger).RootLogger, ShouldHaveSameTypeAs, &log.JSONLogger{})
		})

		Reset(func() {
			canceler()
			<-mainDoneChan
			So(os.Remove(filename), ShouldBeNil)
			So(cl.Close(), ShouldBeNil)
		})
	})
}
