package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/quentin-m/etcd-cloud-operator/pkg/etcd"
	"github.com/signalfx/gateway/config"
	"github.com/signalfx/gateway/protocol/carbon"
	"github.com/signalfx/gateway/protocol/signalfx"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/httpdebug"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/timekeeper"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"runtime"
)

const configEtcd = `
	{
		"LogFormat": "logfmt",
		"LogDir": "-",
		"NumProcs":4,
		"DebugFlag": "debugme",
		"LocalDebugServer": "127.0.0.1:0",
		"ForwardTo": [
			{
				"DefaultAuthToken": "___AUTH_TOKEN___",
				"Name": "testproxy",
				"type": "signalfx-json"
			},
			{
				"Filename": "/tmp/filewrite.csv",
				"Name": "filelocal",
				"type": "csv"
			}
		],
		"ListenFrom": [    ],
		"MaxGracefulWaitTime":     "<<MAX>>ms",
		"GracefulCheckInterval":   "<<CHECK>>ms",
		"MinimalGracefulWaitTime": "<<MIN>>ms",
		"SilentGracefulTime": "50ms",
		"ServerName": "<<SERVERNAME>>",
		"ListenOnPeerAddress": "<<LPADDRESS>>",
		"AdvertisePeerAddress": "<<APADDRESS>>",
		"ListenOnClientAddress": "<<LCADDRESS>>",
		"AdvertiseClientAddress": "<<ACADDRESS>>",
		"ETCDMetricsAddress": "<<MADDRESS>>",
		"UnhealthyMemberTTL": "<<UNHEALTHYTTL>>ms",
		"RemoveMemberTimeout": "<<REMOVEMEMBERTIMEOUT>>ms",
		"ClusterDataDir": "<<DATADIR>>",
		"ClusterOperation": "<<CLUSTEROP>>",
		"TargetClusterAddresses": [
			<<TARGETADDRESSES>>
		]
	}
`

const config1 = `
  {
    "LogFormat": "logfmt",
    "LogDir": "-",
    "NumProcs":4,
    "DebugFlag": "debugme",
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
      },
      {
      	"Type":"signalfx",
      	"ListenAddr": "127.0.0.1:0"
      }
    ],
    "LocalDebugServer": "127.0.0.1:0",
    "ForwardTo":[
      {
      	"Type":"carbon",
      	"Host":"127.0.0.1",
      	"Name": "testForwardTo",
      	"Port": <<PORT>>
      },
    {
      "type": "signalfx-json",
      "DefaultAuthToken": "AAA",
      "url": "http://localhost:9999/v2/datapoint",
      "eventURL": "http://localhost:9999/v2/event",
      "FormatVersion": 3
    }
    ],
	"MaxGracefulWaitTime":     "<<MAX>>ms",
	"GracefulCheckInterval":   "<<CHECK>>ms",
	"MinimalGracefulWaitTime": "<<MIN>>ms",
	"SilentGracefulTime": "50ms"
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
const dupeForwarder = `
  {
    "LogDir": "-",
    "ForwardTo":[
    	{
    		"Type":"signalfx"
		},
    	{
    		"Type":"signalfx"
		}
    ]
  }
`

const dupeListener = `
  {
    "LogDir": "-",
    "ListenFrom":[
    	{
    		"Type":"signalfx"
		},
    	{
    		"Type":"signalfx"
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

const badInnternalMetrics = `
  {
    "ListenFrom":[
    ],
    "ForwardTo":[
    ],
    "InternalMetricsListenerAddress": "0.0.0.0:999999"
  }
`

const internalMetrics = `
  {
    "ListenFrom":[
    ],
    "ForwardTo":[
    ],
    "InternalMetricsListenerAddress": "0.0.0.0:0"
  }
`

const invalidClusterOpConfig = `
  {
    "ListenFrom":[
    ],
    "ForwardTo":[
    ],
    "StatsDelay": "5s",
	"ClusterOperation": "woohoo"
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

func TestMainInstance(t *testing.T) {
	flagParse = func() {}
	s := mainInstance.flags.configFileName
	mainInstance.flags.configFileName = "__INVALID_FILENAME__"
	main()
	mainInstance.flags.configFileName = s
	flagParse = flag.Parse
}

type ConcurrentByteBuffer struct {
	*bytes.Buffer
	lock sync.Mutex
}

func (c *ConcurrentByteBuffer) Write(p []byte) (n int, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.Buffer.Write(p)
}

func failingTestRun(t *testing.T, c string, closeAfterSetup bool, expectedLog string, expectedErr string) context.CancelFunc {
	var cc context.CancelFunc
	Convey("failing config test", t, func() {
		logBuf := &ConcurrentByteBuffer{&bytes.Buffer{}, sync.Mutex{}}
		logger := log.NewHierarchy(log.NewLogfmtLogger(io.MultiWriter(logBuf, os.Stderr), log.Panic))
		logger.Log("config", c, "Trying config")

		fileObj, err := ioutil.TempFile("", "TestProxy")
		So(err, ShouldBeNil)
		filename := fileObj.Name()
		So(os.Remove(filename), ShouldBeNil)
		ctx, contextCancel := context.WithCancel(context.Background())
		So(ioutil.WriteFile(filename, []byte(c), os.FileMode(0666)), ShouldBeNil)
		p := gateway{
			flags: gatewayFlags{
				configFileName: filename,
			},
			logger:          logger,
			tk:              timekeeper.RealTime{},
			setupDoneSignal: make(chan struct{}),
			stdout:          logBuf,
			gomaxprocs:      (&goMaxProcs{}).Set,
			etcdMgr:         &etcdManager{ServerConfig: etcd.ServerConfig{}, logger: logger.CreateChild()},
		}
		if closeAfterSetup {
			go func() {
				<-p.setupDoneSignal
				contextCancel()
			}()
		} else {
			cc = contextCancel
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
	return cc
}

func TestConfigs(t *testing.T) {
	tests := []struct {
		name            string
		config          string
		closeAfterSetup bool
		expectedLog     string
		expectedErr     string
	}{
		{name: "empty", config: emptyConfig, closeAfterSetup: true, expectedLog: "", expectedErr: ""},
		{name: "invalidForwarderConfig", config: invalidForwarderConfig, closeAfterSetup: false, expectedLog: "", expectedErr: "cannot find config unkndfdown"},
		{name: "invalidDebugAddr", config: invalidDebugServerAddr, closeAfterSetup: false, expectedLog: "", expectedErr: "cannot setup debug server"},
		{name: "validInternalMetrics", config: internalMetrics, closeAfterSetup: true, expectedLog: "", expectedErr: ""},
		{name: "invalidInternalMetrics", config: badInnternalMetrics, closeAfterSetup: false, expectedLog: "", expectedErr: "listen tcp: address 999999: invalid port"},
		{name: "invalidJSON", config: "__INVALID__JSON__", closeAfterSetup: false, expectedLog: "", expectedErr: "cannot unmarshal config JSON"},
		{name: "invalidListenerConfig", config: invalidListenerConfig, closeAfterSetup: false, expectedLog: "", expectedErr: "cannot setup listeners from configuration"},
		{name: "invalidPIDfile", config: invalidPIDfile, closeAfterSetup: true, expectedLog: "cannot store pid in pid file", expectedErr: ""},
		{name: "invalidPIDfile", config: invalidClusterOpConfig, closeAfterSetup: true, expectedLog: "", expectedErr: "unsupported cluster-op specified \"woohoo\""},
		{name: "dupeForwarder", config: dupeForwarder, closeAfterSetup: false, expectedLog: "", expectedErr: errDupeForwarder.Error()},
		{name: "dupeListener", config: dupeListener, closeAfterSetup: false, expectedLog: "", expectedErr: errDupeListener.Error()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failingTestRun(t, test.config, test.closeAfterSetup, test.expectedLog, test.expectedErr)
		})
	}
}

func TestForwarderName(t *testing.T) {
	Convey("Forwarder names", t, func() {
		So(forwarderName(&config.ForwardTo{Name: pointer.String("bob")}), ShouldEqual, "bob")
		So(forwarderName(&config.ForwardTo{Type: "atype"}), ShouldEqual, "atype")
	})
}

func TestProxy1(t *testing.T) {
	var cancelfunc context.CancelFunc
	Convey("a setup carbon gateway", t, func() {
		sendTo := dptest.NewBasicSink()
		var ctx context.Context
		ctx, cancelfunc = context.WithCancel(context.Background())
		var p *gateway
		var mainDoneChan chan error
		var filename string
		var cl *carbon.Listener
		checkError := false

		setUp := func(max int, min int, check int) {
			go func() {
				for {
					ln, _ := net.Listen("tcp", "localhost:9999")
					if ln != nil {
						conn, _ := ln.Accept()
						time.Sleep(time.Second)
						_ = conn.Close()
					}
				}
			}()

			logBuf := &ConcurrentByteBuffer{&bytes.Buffer{}, sync.Mutex{}}
			logger := log.NewHierarchy(log.NewLogfmtLogger(io.MultiWriter(logBuf, os.Stderr), log.Panic))
			fileObj, err := ioutil.TempFile("", "TestProxy")
			So(err, ShouldBeNil)
			etcdDataDir, err := ioutil.TempDir("", "TestProxy1")
			So(err, ShouldBeNil)
			So(os.RemoveAll(etcdDataDir), ShouldBeNil)
			filename = fileObj.Name()
			So(os.Remove(filename), ShouldBeNil)
			cconf := &carbon.ListenerConfig{}
			cl, err = carbon.NewListener(sendTo, cconf)
			So(err, ShouldBeNil)
			openPort := nettest.TCPPort(cl)
			proxyConf := strings.Replace(config1, "<<PORT>>", strconv.FormatInt(int64(openPort), 10), -1)
			proxyConf = strings.Replace(proxyConf, "<<MAX>>", strconv.FormatInt(int64(max), 10), -1)
			proxyConf = strings.Replace(proxyConf, "<<MIN>>", strconv.FormatInt(int64(min), 10), -1)
			proxyConf = strings.Replace(proxyConf, "<<CHECK>>", strconv.FormatInt(int64(check), 10), -1)
			So(ioutil.WriteFile(filename, []byte(proxyConf), os.FileMode(0666)), ShouldBeNil)
			fmt.Println("Launching server...")
			gmp := &goMaxProcs{}
			p = &gateway{
				flags: gatewayFlags{
					configFileName: filename,
				},
				stdout:          os.Stdout,
				logger:          logger,
				tk:              timekeeper.RealTime{},
				setupDoneSignal: make(chan struct{}),
				gomaxprocs:      gmp.Set,
				signalChan:      make(chan os.Signal),
				etcdMgr:         &etcdManager{ServerConfig: etcd.ServerConfig{DataDir: etcdDataDir, LCAddress: "127.0.0.1:2379", ACAddress: "127.0.0.1:2379", LPAddress: "127.0.0.1:2380", APAddress: "127.0.0.1:2380", MAddress: "127.0.0.1:2381"}, operation: "", logger: logger.CreateChild()},
			}
			mainDoneChan = make(chan error)
			go func() {
				mainDoneChan <- p.main(ctx)
				close(mainDoneChan)
			}()
			<-p.setupDoneSignal
			So(gmp.lastVal, ShouldEqual, int64(4))
		}

		Convey("should have signalfx listener too", func() {
			setUp(1000, 0, 25)
			So(p, ShouldNotBeNil)
			sfxListenPort := nettest.TCPPort(p.listeners[2].(*signalfx.ListenerServer))
			resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/v2/datapoint", sfxListenPort), "application/json", strings.NewReader("{}"))
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
			So(resp.Header.Get("X-Response-Id"), ShouldNotEqual, "")
		})

		Convey("should have debug values", func() {
			setUp(1000, 0, 25)
			So(p, ShouldNotBeNil)
			listenPort := nettest.TCPPort(p.debugServerListener)
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/debug/vars", listenPort))
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
		})

		Convey("should gateway a carbon point", func() {
			setUp(1000, 0, 25)
			So(p, ShouldNotBeNil)
			dp := dptest.DP()
			dp.Dimensions = nil
			dp.Timestamp = dp.Timestamp.Round(time.Second)
			So(p.forwarders[0].AddDatapoints(ctx, []*datapoint.Datapoint{dp}), ShouldBeNil)
			seenDatapoint := sendTo.Next()
			So(seenDatapoint, ShouldNotBeNil)
		})

		Convey("getLogOutput should work correctly", func() {
			setUp(1000, 0, 25)
			So(p, ShouldNotBeNil)
			So(p.getLogOutput(&config.GatewayConfig{
				LogDir: pointer.String("-"),
			}), ShouldEqual, os.Stdout)
			So(p.getLogOutput(&config.GatewayConfig{
				LogDir:        pointer.String(""),
				LogMaxSize:    pointer.Int(0),
				LogMaxBackups: pointer.Int(0),
			}), ShouldNotEqual, os.Stdout)
			l := p.getLogger(&config.GatewayConfig{
				LogDir:    pointer.String("-"),
				LogFormat: pointer.String("json"),
			})
			So(l.(*log.ErrorLogLogger).RootLogger, ShouldHaveSameTypeAs, &log.JSONLogger{})
		})

		Convey("max time exceed should work correctly", func() {
			setUp(0, 0, 25)
			So(p, ShouldNotBeNil)
		})

		Convey("", func() {
			setUp(1000, 0, 25)
			So(p, ShouldNotBeNil)
			sfxListenPort := nettest.TCPPort(p.listeners[2].(*signalfx.ListenerServer))
			go func() {
				for {
					_, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/v2/datapoint", sfxListenPort), "application/json", strings.NewReader("{ \"gauge\": [{ \"metric\": \"test.gauge\", \"dimensions\": { \"host\": \"testserver\" }, \"value\": 42 }]}"))
					log.IfErr(log.Discard, err)
					time.Sleep(time.Millisecond)
				}
			}()
			for p.Pipeline() == 0 {
				time.Sleep(time.Millisecond * 10)
			}

		})

		Reset(func() {
			p.signalChan <- syscall.SIGTERM
			time.Sleep(time.Millisecond)
			err := <-mainDoneChan
			if checkError {
				So(err, ShouldNotBeNil)
				checkError = false
			}
			So(os.Remove(filename), ShouldBeNil)
			So(cl.Close(), ShouldBeNil)
			if cancelfunc != nil {
				cancelfunc()
			}
		})
	})
}

func startProxies(ctx context.Context, proxies []*gateway, logger *log.Hierarchy) ([]*gateway, []chan error) {
	ps := make([]*gateway, 0, len(proxies))
	mainDoneChans := make([]chan error, 0, len(proxies))
	gmp := &goMaxProcs{}
	for _, p := range proxies {
		p.gomaxprocs = gmp.Set
		p.logger = logger.CreateChild()
		p.etcdMgr.logger = logger.CreateChild()
	retry:
		mainDoneChan := make(chan error)
		go func(p *gateway, mainDoneChan chan error) {
			mainDoneChan <- p.main(ctx)
			close(mainDoneChan)
		}(p, mainDoneChan)
		select {
		case <-p.setupDoneSignal:
			mainDoneChans = append(mainDoneChans, mainDoneChan)
			ps = append(ps, p)
			continue
		case err := <-mainDoneChan:
			if err != nil {
				p.etcdMgr.shutdown(false)
				time.Sleep(time.Second * 5)
				goto retry
			}
		}
	}
	return ps, mainDoneChans
}

func formatTargetAddresses(targetClusters []string) (targetAddresses string) {
	for index, address := range targetClusters {
		targetAddresses += fmt.Sprintf("\"%s\"", address)
		if index < (len(targetClusters) - 1) {
			targetAddresses += ","
		}
		targetAddresses += "\n"
	}
	return
}

var errTest = errors.New("test")

func Test_NonNil(t *testing.T) {
	assert.Equal(t, FirstNonNil(errTest), errTest)
}

func TestProxyCluster(t *testing.T) {
	Convey("a setup gateway cluster", t, func() {
		ctx, cancelfunc := context.WithCancel(context.Background())
		var ps []*gateway
		filenames := make([]string, 0, 0)
		var mainDoneChans []chan error
		logBuf := &ConcurrentByteBuffer{&bytes.Buffer{}, sync.Mutex{}}
		checkError := false

		setUp := func(max int, min int, check int, etcdConfigs []*etcdManager) {
			go func() {
				for {
					ln, _ := net.Listen("tcp", "localhost:9999")
					if ln != nil {
						conn, _ := ln.Accept()
						time.Sleep(time.Second)
						_ = conn.Close()
					}
				}
			}()
			var proxies = make([]*gateway, 0, len(etcdConfigs))
			for _, etcdConf := range etcdConfigs {
				fileObj, err := ioutil.TempFile("", "TestProxyCluster")
				So(err, ShouldBeNil)
				etcdDataDir, err := ioutil.TempDir("", "TestProxyCluster")
				So(err, ShouldBeNil)
				So(os.RemoveAll(etcdDataDir), ShouldBeNil)
				filename := fileObj.Name()
				filenames = append(filenames, filename)
				So(os.Remove(filename), ShouldBeNil)
				proxyConf := configEtcd
				proxyConf = strings.Replace(proxyConf, "<<MAX>>", strconv.FormatInt(int64(max), 10), -1)
				proxyConf = strings.Replace(proxyConf, "<<MIN>>", strconv.FormatInt(int64(min), 10), -1)
				proxyConf = strings.Replace(proxyConf, "<<CHECK>>", strconv.FormatInt(int64(check), 10), -1)
				proxyConf = strings.Replace(proxyConf, "<<CHECK>>", strconv.FormatInt(int64(check), 10), -1)
				proxyConf = strings.Replace(proxyConf, "<<LPADDRESS>>", etcdConf.LPAddress, -1)
				proxyConf = strings.Replace(proxyConf, "<<APADDRESS>>", etcdConf.APAddress, -1)
				proxyConf = strings.Replace(proxyConf, "<<LCADDRESS>>", etcdConf.LCAddress, -1)
				proxyConf = strings.Replace(proxyConf, "<<ACADDRESS>>", etcdConf.ACAddress, -1)
				proxyConf = strings.Replace(proxyConf, "<<MADDRESS>>", etcdConf.MAddress, -1)
				proxyConf = strings.Replace(proxyConf, "<<UNHEALTHYTTL>>", strconv.FormatFloat(etcdConf.UnhealthyMemberTTL.Seconds()*1000, 'f', 2, 64), -1)
				proxyConf = strings.Replace(proxyConf, "<<REMOVEMEMBERTIMEOUT>>", strconv.FormatInt(int64(etcdConf.removeTimeout), 10), -1)
				proxyConf = strings.Replace(proxyConf, "<<DATADIR>>", filepath.Join(etcdDataDir, etcdConf.DataDir), -1)
				proxyConf = strings.Replace(proxyConf, "<<CLUSTEROP>>", etcdConf.operation, -1)
				proxyConf = strings.Replace(proxyConf, "<<TARGETADDRESSES>>", formatTargetAddresses(etcdConf.targetCluster), -1)
				proxyConf = strings.Replace(proxyConf, "<<SERVERNAME>>", etcdConf.Name, -1)

				So(ioutil.WriteFile(filename, []byte(proxyConf), os.FileMode(0666)), ShouldBeNil)

				proxies = append(proxies, &gateway{
					stdout:          os.Stdout,
					tk:              timekeeper.RealTime{},
					setupDoneSignal: make(chan struct{}),
					signalChan:      make(chan os.Signal),
					flags: gatewayFlags{
						configFileName: filename,
					},
					etcdMgr: &etcdManager{ServerConfig: etcd.ServerConfig{}},
				})
			}
			logger := log.NewHierarchy(log.NewLogfmtLogger(io.MultiWriter(logBuf, os.Stderr), log.Panic))
			fmt.Println("Launching servers...")
			ps, mainDoneChans = startProxies(ctx, proxies, logger)
		}

		Convey("the etcd cluster should be aware of all members", func() {
			etcdConfs := []*etcdManager{
				{removeTimeout: 3000, ServerConfig: etcd.ServerConfig{Name: "instance1", UnhealthyMemberTTL: 1000 * time.Millisecond, DataDir: "etcd-data", LCAddress: "127.0.0.1:2379", ACAddress: "127.0.0.1:2379", LPAddress: "127.0.0.1:2380", APAddress: "127.0.0.1:2380", MAddress: "127.0.0.1:2381"}, operation: "seed"},
				{removeTimeout: 3000, ServerConfig: etcd.ServerConfig{Name: "instance2", UnhealthyMemberTTL: 1000 * time.Millisecond, DataDir: "etcd-data1", LCAddress: "127.0.0.1:2479", ACAddress: "127.0.0.1:2479", LPAddress: "127.0.0.1:2480", APAddress: "127.0.0.1:2480", MAddress: "127.0.0.1:2481"}, targetCluster: []string{"127.0.0.1:2379"}, operation: "join"},
				{removeTimeout: -1, ServerConfig: etcd.ServerConfig{Name: "instance3", UnhealthyMemberTTL: 1000 * time.Millisecond, DataDir: "etcd-data2", LCAddress: "127.0.0.1:2579", ACAddress: "127.0.0.1:2579", LPAddress: "127.0.0.1:2580", APAddress: "127.0.0.1:2580", MAddress: "127.0.0.1:2581"}, targetCluster: []string{"127.0.0.1:2379", "127.0.0.1:2479"}, operation: "join"},
				{removeTimeout: 3000, ServerConfig: etcd.ServerConfig{Name: "", UnhealthyMemberTTL: 1000 * time.Millisecond, DataDir: "etcd-data3", LCAddress: "127.0.0.1:2679", ACAddress: "127.0.0.1:2679", LPAddress: "127.0.0.1:2680", APAddress: "127.0.0.1:2680", MAddress: "127.0.0.1:2681"}, targetCluster: []string{"127.0.0.1:2379", "127.0.0.1:2479", "127.0.0.1:2579"}, operation: "join"},
			}
			setUp(15000, 0, 25, etcdConfs)
			So(ps[0].etcdMgr.server.IsRunning(), ShouldBeTrue)
			for _, p := range ps {
				memberList, err := p.etcdMgr.client.MemberList(context.Background())
				fmt.Println(memberList)
				So(err, ShouldBeNil)
				So(len(memberList.Members), ShouldEqual, len(ps))
			}
		})

		Reset(func() {
			for index, p := range ps {
				p.signalChan <- syscall.SIGTERM
				err := <-mainDoneChans[index]
				if checkError {
					So(err, ShouldNotBeNil)
					checkError = false
				}
			}
			for _, filename := range filenames {
				So(os.Remove(filename), ShouldBeNil)
			}
			if cancelfunc != nil {
				cancelfunc()
			}
		})
	})
}

func TestStringIsInSlice(t *testing.T) {
	Convey("stringIsInSlice", t, func() {
		testData := []string{"hello", "world"}
		Convey("should return true if the string is in the slice", func() {
			So(isStringInSlice("hello", testData), ShouldBeTrue)
		})
		Convey("should return false if the string is not in the slice", func() {
			So(isStringInSlice("goodbye", []string{}), ShouldBeFalse)
			So(isStringInSlice("goodbye", testData), ShouldBeFalse)
		})
	})
}

func TestEnvVarFuncs(t *testing.T) {
	testKey := "SFX_TEST_ENV_VAR"
	Convey("test the following environment variable helper functions", t, func() {
		Convey("getCommaSeparatedStringEnvVar should parses comma separated strings", func() {
			testVal := []string{"127.0.0.1:9999", "127.0.0.2:9999", "127.0.0.3:9999"}
			os.Setenv(testKey, strings.Join(testVal, ","))
			loaded := getCommaSeparatedStringEnvVar(testKey, []string{})
			So(len(loaded), ShouldEqual, 3)
			So(strings.Join(loaded, ","), ShouldEqual, strings.Join(testVal, ","))
		})
		Convey("getStringEnvVar", func() {
			Convey("should return the value if the environment variable is set", func() {
				testVal := "testStringValue"
				os.Setenv(testKey, testVal)
				loaded := getStringEnvVar(testKey, "defaultVal")
				So(loaded, ShouldEqual, testVal)
			})
			Convey("should return the default value if the environment variable is not set", func() {
				loaded := getStringEnvVar(testKey, "defaultVal")
				So(loaded, ShouldEqual, "defaultVal")
			})
		})
		Convey("getDurationEnvVar", func() {
			Convey("should return the value if the environment variable is set", func() {
				testVal := "5s"
				os.Setenv(testKey, testVal)
				loaded := getDurationEnvVar(testKey, 0*time.Second)
				So(loaded, ShouldEqual, time.Second*5)
			})
			Convey("should return the default value if the environment variable is not set", func() {
				loaded := getDurationEnvVar(testKey, 1*time.Second)
				So(loaded, ShouldEqual, time.Second*1)
			})
		})
		Reset(func() {
			os.Unsetenv(testKey)
		})
	})
}

// there will be a test on this later
type addfunc func(http.ResponseWriter, *http.Request)

func (f addfunc) DebugEndpoints() map[string]http.Handler {
	return map[string]http.Handler{"/sampler": http.HandlerFunc(f)}
}

func debugEndpointHandleFunc(rw http.ResponseWriter, req *http.Request) {
	rw.Write([]byte(`"OK"`))
}

func TestDebugEndpoints(t *testing.T) {
	Convey("test handle endpoints", t, func() {
		listener, err := net.Listen("tcp", "0.0.0.0:0")
		So(err, ShouldBeNil)
		p := &gateway{}
		p.debugServerListener = listener
		p.debugServer = httpdebug.New(&httpdebug.Config{
			Logger:        log.DefaultLogger,
			ExplorableObj: p,
		})
		m := map[string]http.Handler{}

		// casting func to type addfunc so it can satisfy the protocol.DebugEndpointer interface, which in turn casts the func itself to be an http.HandlerFunc which implements the http.Handler interface
		p.addEndpoints(addfunc(debugEndpointHandleFunc), m)

		p.handleEndpoints(m)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			p.debugServer.Serve(listener)
			wg.Done()
		}()
		runtime.Gosched()
		listenPort := nettest.TCPPort(p.debugServerListener)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/sampler", listenPort))
		So(err, ShouldBeNil)
		So(resp.StatusCode, ShouldEqual, 200)
		So(p.debugServerListener.Close(), ShouldBeNil)
		wg.Wait()
	})
}
