package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/protocol/carbon"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const config1 = `
  {
    "LogFormat": "logfmt",
    "LogDir": "-",
    "NumProcs":4,
    "ListenFrom":[
      {
      	"Type":"carbon",
      	"ListenAddr": "127.0.0.1:0"
	  },
      {
      	"Type":"carbon",
      	"Name": "duplicate listener",
      	"ListenAddr": "127.0.0.1:0",
        "MetricDeconstructor": "commakeys",
        "MetricDeconstructorOptions": "mtypedim:metrictype"
	  }
    ],
    "LocalDebugServer": "127.0.0.1:0",
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
    "LogDir": "-",
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
    "LogDir": "-",
    "ListenFrom":[
    	{
    		"Type":"unknown"
		}
    ],
    "ForwardTo":[
    ]
  }
`

const invalidPIDfile = `
  {
    "LogDir": "-",
    "PidFilename":"/",
    "ListenFrom":[
    ],
    "ForwardTo":[
    ]
  }
`

const emptyConfig = `
  {
    "ListenFrom":[
    ],
    "ForwardTo":[
    ],
    "StatsDelay": "5s"
  }
`

const invalidDebugServerAddr = `
  {
    "LogDir": "-",
    "LocalDebugServer":"127.0.0.1:999999",
    "ListenFrom":[
    ],
    "ForwardTo":[
    ]
  }
`

type goMaxProcs struct {
	lastVal int64
}

func (g *goMaxProcs) Set(i int) int {
	return int(atomic.SwapInt64(&g.lastVal, int64(i)))
}

func TestOsHostname(t *testing.T) {
	Convey("failing config test", t, func() {
		So(getHostname(func() (string, error) {
			return "", errors.New("nope")
		}), ShouldEqual, "unknown")
		So(getHostname(func() (string, error) {
			return "bob", nil
		}), ShouldEqual, "bob")
	})
}

func TestMainInstance(t *testing.T) {
	flagParse = func() {}
	s := mainInstance.flags.configFileName
	mainInstance.flags.configFileName = "__INVALID_FILENAME__"
	main()
	mainInstance.flags.configFileName = s
	flagParse = flag.Parse
}

func failingTestRun(t *testing.T, c string, closeAfterSetup bool, expectedLog string, expectedErr string) {
	Convey("failing config test", t, func() {
		logBuf := &bytes.Buffer{}
		logger := log.NewHierarchy(log.NewLogfmtLogger(io.MultiWriter(logBuf, os.Stderr), log.Panic))
		logger.Log("config", c, "Trying config")

		fileObj, err := ioutil.TempFile("", "TestProxy")
		So(err, ShouldBeNil)
		filename := fileObj.Name()
		So(os.Remove(filename), ShouldBeNil)
		ctx, contextCancel := context.WithCancel(context.Background())
		So(ioutil.WriteFile(filename, []byte(c), os.FileMode(0666)), ShouldBeNil)
		p := proxy{
			flags: proxyFlags{
				configFileName: filename,
			},
			logger:          logger,
			tk:              timekeeper.RealTime{},
			setupDoneSignal: make(chan struct{}),
			stdout:          logBuf,
			gomaxprocs:      (&goMaxProcs{}).Set,
		}
		if closeAfterSetup {
			go func() {
				<-p.setupDoneSignal
				contextCancel()
			}()
		}
		err = p.main(ctx)
		if expectedErr != "" {
			So(err, ShouldNotBeNil)
			So(errors.Details(err), ShouldContainSubstring, expectedErr)
		} else {
			So(err, ShouldBeNil)
		}
		if expectedLog != "" {
			So(logBuf.String(), ShouldContainSubstring, expectedLog)
		}
	})
}

func TestEmptyConfig(t *testing.T) {
	failingTestRun(t, emptyConfig, true, "", "")
}

func TestInvalidConfigForwarder(t *testing.T) {
	failingTestRun(t, invalidForwarderConfig, false, "", "cannot find config unkndfdown")
}

func TestInvalidConfigJSON(t *testing.T) {
	failingTestRun(t, "__INVALID__JSON__", false, "", "cannot unmarshal config JSON")
}

func TestInvalidConfigListener(t *testing.T) {
	failingTestRun(t, invalidListenerConfig, false, "", "cannot setup listeners from configuration")
}

func TestInvalidConfigDebugAddr(t *testing.T) {
	failingTestRun(t, invalidDebugServerAddr, false, "", "cannot setup debug server")
}

func TestInvalidConfigPIDFile(t *testing.T) {
	failingTestRun(t, invalidPIDfile, true, "cannot store pid in pid file", "")
}

func TestForwarderName(t *testing.T) {
	Convey("Forwarder names", t, func() {
		So(forwarderName(&config.ForwardTo{Name: pointer.String("bob")}), ShouldEqual, "bob")
		So(forwarderName(&config.ForwardTo{Type: "atype"}), ShouldEqual, "atype")
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

		gmp := &goMaxProcs{}
		p := proxy{
			flags: proxyFlags{
				configFileName: filename,
			},
			stdout:          os.Stdout,
			logger:          log.NewHierarchy(log.NewLogfmtLogger(io.MultiWriter(logBuf, os.Stderr), log.Panic)),
			tk:              timekeeper.RealTime{},
			setupDoneSignal: make(chan struct{}),
			gomaxprocs:      gmp.Set,
		}
		mainDoneChan := make(chan struct{})
		go func() {
			p.main(ctx)
			close(mainDoneChan)
		}()
		<-p.setupDoneSignal
		listeningCarbonProxyPort := nettest.TCPPort(p.listeners[0].(*carbon.Listener))
		So(gmp.lastVal, ShouldEqual, int64(4))

		Convey("should have debug values", func() {
			listenPort := nettest.TCPPort(p.debugServer)
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/debug/vars", listenPort))
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
		})

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
				LogDir:        pointer.String(""),
				LogMaxSize:    pointer.Int(0),
				LogMaxBackups: pointer.Int(0),
			}), ShouldNotEqual, os.Stdout)
			l := p.getLogger(&config.ProxyConfig{
				LogDir:    pointer.String("-"),
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
