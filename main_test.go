package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/signalfx/gateway/flaghelpers"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coreos/etcd/embed"
	"github.com/signalfx/embetcd/embetcd"
	"github.com/signalfx/gateway/config"
	"github.com/signalfx/gateway/protocol/carbon"
	"github.com/signalfx/gateway/protocol/signalfx"
	_ "github.com/signalfx/go-metrics"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/httpdebug"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/timekeeper"
	_ "github.com/signalfx/ondiskencoding"
	. "github.com/smartystreets/goconvey/convey"
	_ "github.com/spaolacci/murmur3"
	"gotest.tools/assert"
)

const configEtcd = `
	{
		"ClusterName": "<<CLUSTERNAME>>",
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
	"SilentGracefulTime": "50ms",
	"InternalMetricsListenerAddress": "<<INTERNALMETRICS>>"
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

func Test_setupGoMaxProcs(t *testing.T) {
	Convey("setup go max procs", t, func() {
		gmp := &goMaxProcs{lastVal: 0}
		setupGoMaxProcs(&config.GatewayConfig{}, gmp.Set)
		So(gmp.lastVal, ShouldEqual, runtime.NumCPU())
		setupGoMaxProcs(&config.GatewayConfig{NumProcs: pointer.Int(3)}, gmp.Set)
		So(gmp.lastVal, ShouldEqual, 3)

	})
}

func Test_GetContext(t *testing.T) {
	Convey("GetContext", t, func() {
		So(GetContext(nil), ShouldEqual, context.Background())
		So(GetContext(context.Background()), ShouldEqual, context.Background())
	})
}

func Test_Main(t *testing.T) {
	defer func() {
		// reset package flags
		flags = &gatewayFlags{}
		flagParse = flag.Parse
	}()
	flags.configFileName = "__INVALID_FILENAME__"
	main()
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

func Test_logIfCtxExceeded(t *testing.T) {
	Convey("logIfCtxExceeded", t, func() {
		logBuf := &ConcurrentByteBuffer{&bytes.Buffer{}, sync.Mutex{}}
		logger := log.NewHierarchy(log.NewLogfmtLogger(io.MultiWriter(logBuf, os.Stderr), log.Panic))
		ctx, cancel := context.WithCancel(context.Background())
		logIfCtxExceeded(ctx, logger)
		So(logBuf.String(), ShouldContainSubstring, "Graceful shutdown complete")
		logBuf.Reset()
		cancel()
		logIfCtxExceeded(ctx, logger)
		So(logBuf.String(), ShouldContainSubstring, "Exceeded graceful shutdown period")
	})
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
			tk:              timekeeper.RealTime{},
			setupDoneSignal: make(chan struct{}),
			stdout:          logBuf,
		}
		if closeAfterSetup {
			go func() {
				<-p.setupDoneSignal
				contextCancel()
			}()
		} else {
			cc = contextCancel
		}

		// loadedConfig
		var loadedConfig *config.GatewayConfig
		loadedConfig, err = loadConfig(filename, logger)

		// set config on gateway instance
		if loadedConfig != nil {
			writePidFile(loadedConfig, logger)
			p.configure(loadedConfig)
			err = p.start(ctx)
		}
		if expectedErr != "" {
			fmt.Println(logBuf.String())
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
		{name: "invalidClusterOp", config: invalidClusterOpConfig, closeAfterSetup: true, expectedLog: "", expectedErr: "unsupported cluster-op specified \"woohoo\""},
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

		setUp := func(max int, min int, check int, internalMetricsAddress string) {
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
			proxyConf = strings.Replace(proxyConf, "<<INTERNALMETRICS>>", internalMetricsAddress, -1)
			So(ioutil.WriteFile(filename, []byte(proxyConf), os.FileMode(0666)), ShouldBeNil)
			fmt.Println("Launching server...")
			p = newGateway()
			p.logger = logger
			loadedConfig, _ := loadConfig(filename, logger)
			p.configure(loadedConfig)
			mainDoneChan = make(chan error)
			go func() {
				mainDoneChan <- p.start(ctx)
				close(mainDoneChan)
			}()
			<-p.setupDoneSignal
		}

		Convey("should have signalfx listener too", func() {
			setUp(1000, 0, 25, "0.0.0.0:2500")
			So(p, ShouldNotBeNil)
			sfxListenPort := nettest.TCPPort(p.listeners[2].(*signalfx.ListenerServer))
			resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/v2/datapoint", sfxListenPort), "application/json", strings.NewReader("{}"))
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
			So(resp.Header.Get("X-Response-Id"), ShouldNotEqual, "")
		})

		Convey("should have debug values", func() {
			setUp(1000, 0, 25, "0.0.0.0:2501")
			So(p, ShouldNotBeNil)
			listenPort := nettest.TCPPort(p.debugServerListener)
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/debug/vars", listenPort))
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
		})

		Convey("should gateway a carbon point", func() {
			setUp(1000, 0, 25, "0.0.0.0:2501")
			So(p, ShouldNotBeNil)
			dp := dptest.DP()
			dp.Dimensions = nil
			dp.Timestamp = dp.Timestamp.Round(time.Second)
			So(p.forwarders[0].AddDatapoints(ctx, []*datapoint.Datapoint{dp}), ShouldBeNil)
			seenDatapoint := sendTo.Next()
			So(seenDatapoint, ShouldNotBeNil)
		})

		Convey("getLogOutput should work correctly", func() {
			setUp(1000, 0, 25, "0.0.0.0:2502")
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
			setUp(0, 0, 25, "0.0.0.0:2503")
			So(p, ShouldNotBeNil)
		})

		Convey("", func() {
			setUp(1000, 0, 25, "0.0.0.0:2504")
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

var errTest = errors.New("test")

func Test_NonNil(t *testing.T) {
	assert.Equal(t, FirstNonNil(errTest), errTest)
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

// The following functions are test fixtures for cluster tests
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

/* Cluster Tests */

// The following functions are test fixtures for TestProxyCluster
func setConfigFile(etcdConf *embetcd.Config, max int, min int, check int) (configFilePath string, tempDir string) {
	// get a temporary directory for the etcd data directory
	tempDir, err := ioutil.TempDir("", "TestProxyCluster")
	So(err, ShouldBeNil)
	So(os.RemoveAll(tempDir), ShouldBeNil)

	// get a temporary filename for the config file
	fileObj, err := ioutil.TempFile("", "TestProxyClusterConfig")
	So(err, ShouldBeNil)
	configFilePath = fileObj.Name()
	// remove the temp file so we can overwrite it
	So(os.Remove(configFilePath), ShouldBeNil)

	//// remove the temp dir so we can recreate it

	proxyConf := configEtcd
	proxyConf = strings.Replace(proxyConf, "<<MAX>>", strconv.FormatInt(int64(max), 10), -1)
	proxyConf = strings.Replace(proxyConf, "<<MIN>>", strconv.FormatInt(int64(min), 10), -1)
	proxyConf = strings.Replace(proxyConf, "<<CHECK>>", strconv.FormatInt(int64(check), 10), -1)
	proxyConf = strings.Replace(proxyConf, "<<LPADDRESS>>", etcdConf.LPUrls[0].String(), -1)
	proxyConf = strings.Replace(proxyConf, "<<APADDRESS>>", etcdConf.APUrls[0].String(), -1)
	proxyConf = strings.Replace(proxyConf, "<<LCADDRESS>>", etcdConf.LCUrls[0].String(), -1)
	proxyConf = strings.Replace(proxyConf, "<<ACADDRESS>>", etcdConf.ACUrls[0].String(), -1)
	proxyConf = strings.Replace(proxyConf, "<<MADDRESS>>", etcdConf.ListenMetricsUrls[0].String(), -1)
	proxyConf = strings.Replace(proxyConf, "<<UNHEALTHYTTL>>", etcdConf.UnhealthyTTL.String(), -1)
	proxyConf = strings.Replace(proxyConf, "<<REMOVEMEMBERTIMEOUT>>", etcdConf.RemoveMemberTimeout.String(), -1)
	proxyConf = strings.Replace(proxyConf, "<<DATADIR>>", filepath.Join(tempDir, etcdConf.Dir), -1)
	proxyConf = strings.Replace(proxyConf, "<<CLUSTEROP>>", etcdConf.ClusterState, -1)
	proxyConf = strings.Replace(proxyConf, "<<TARGETADDRESSES>>", formatTargetAddresses(etcdConf.InitialCluster), -1)
	proxyConf = strings.Replace(proxyConf, "<<SERVERNAME>>", etcdConf.Name, -1)
	proxyConf = strings.Replace(proxyConf, "<<CLUSTERNAME>>", etcdConf.ClusterName, -1)

	So(ioutil.WriteFile(path.Join(configFilePath), []byte(proxyConf), os.FileMode(0666)), ShouldBeNil)
	return configFilePath, tempDir
}

// startTestGateway starts a gateway and waits for it to signal that setup is down or for the
// main function to return an error
func startTestGateway(ctx context.Context, gw *gateway) chan error {
	mainErrCh := make(chan error, 1)
	go func() {
		mainErrCh <- gw.start(ctx)
		close(mainErrCh)
	}()

	// wait for the gateway to start or error out
	select {
	case <-gw.setupDoneSignal:
	case <-mainErrCh:
	}

	return mainErrCh
}

func TestProxyCluster(t *testing.T) {
	Convey("the etcd cluster should...", t, func() {
		// initialize storage test structures
		var configFiles []string
		var etcdDataDirs []string
		var ctx context.Context
		var cancel context.CancelFunc

		Convey("be aware of all members", func() {
			etcdConfigs := []*embetcd.Config{
				{Config: &embed.Config{Name: "instance1", Dir: "etcd-data", ClusterState: "seed", LCUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2379"}}, ACUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2379"}}, LPUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2380"}}, APUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2380"}}, ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2381"}}}, ClusterName: "test-cluster-1", InitialCluster: []string{}, RemoveMemberTimeout: pointer.Duration(3000 * time.Second), UnhealthyTTL: pointer.Duration(1000 * time.Millisecond)},
				{Config: &embed.Config{Name: "instance2", Dir: "etcd-data1", ClusterState: "join", LCUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2479"}}, ACUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2479"}}, LPUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2480"}}, APUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2480"}}, ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2481"}}}, ClusterName: "test-cluster-1", InitialCluster: []string{"127.0.0.1:2379", "127.0.0.1:2479", "127.0.0.1:2579"}, RemoveMemberTimeout: pointer.Duration(3000 * time.Second), UnhealthyTTL: pointer.Duration(1000 * time.Millisecond)},
				{Config: &embed.Config{Name: "instance3", Dir: "etcd-data2", ClusterState: "join", LCUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2579"}}, ACUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2579"}}, LPUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2580"}}, APUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2580"}}, ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2581"}}}, ClusterName: "test-cluster-1", InitialCluster: []string{"127.0.0.1:2379"}, RemoveMemberTimeout: pointer.Duration(-1 * time.Second), UnhealthyTTL: pointer.Duration(1000 * time.Millisecond)},
				{Config: &embed.Config{Name: "instance4", Dir: "etcd-data3", ClusterState: "client", LCUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2679"}}, ACUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2679"}}, LPUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2680"}}, APUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2680"}}, ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2681"}}}, ClusterName: "test-cluster-1", InitialCluster: []string{"127.0.0.1:2379", "127.0.0.1:2479", "127.0.0.1:2579"}, RemoveMemberTimeout: pointer.Duration(3000 * time.Second), UnhealthyTTL: pointer.Duration(1000 * time.Millisecond)},
			}

			gateways := make([]*gateway, 0, len(etcdConfigs))
			mainErrChs := make([]chan error, 0, len(etcdConfigs))
			configFiles = make([]string, 0, len(etcdConfigs))
			etcdDataDirs = make([]string, 0, len(etcdConfigs))

			// set up logger
			logBuf := &ConcurrentByteBuffer{&bytes.Buffer{}, sync.Mutex{}}
			logger := log.NewHierarchy(log.NewLogfmtLogger(io.MultiWriter(logBuf, os.Stderr), log.Panic))

			// test context
			ctx, cancel = context.WithCancel(context.Background())

			var numClients int

			// set up config files
			for index, config := range etcdConfigs {

				// create the configuration file
				configFile, etcdDataDir := setConfigFile(config, 5000, 0, 25)
				configFiles = append(configFiles, configFile)
				etcdDataDirs = append(etcdDataDirs, etcdDataDir)

				// create gateway struct with config file path set as a flag
				gw := &gateway{
					logger:          logger.CreateChild(),
					stdout:          os.Stdout,
					tk:              timekeeper.RealTime{},
					setupDoneSignal: make(chan struct{}),
					signalChan:      make(chan os.Signal),
				}

				flags = &gatewayFlags{}
				flags.operation = flaghelpers.NewStringFlag()
				if index == 0 {
					flags.operation.Set("seed")
				}

				loadedConfig, _ := loadConfig(configFile, logger)
				loadedConfig.EtcdServerStartTimeout = pointer.Duration(time.Second * 60)

				gw.configure(loadedConfig)

				// start the gateway
				mainErrChs = append(mainErrChs, startTestGateway(ctx, gw))
				gateways = append(gateways, gw)

				// verify that the gateway started successfully
				if config.ClusterState != "client" {
					So(gw.etcdServer, ShouldNotBeNil)
					So(gw.etcdServer.IsRunning(), ShouldBeTrue)
				} else {
					numClients++
					So(gw.etcdServer, ShouldBeNil)
				}
				// there should always be a client
				So(gw.etcdClient, ShouldNotBeNil)
			}

			// check that each instance is aware of each other
			// take into account that one of the instances is a client only and does not have members
			for _, gw := range gateways {
				if gw.etcdServer != nil {
					// There's only one member that is a client and not running the etcd server we must account for this
					So(len(gw.etcdServer.Server.Cluster().Members()), ShouldEqual, len(gateways)-numClients)
				}
				if gw.etcdClient != nil {
					resp, err := gw.etcdClient.Get(ctx, "__etcd-cluster__", clientv3.WithPrefix())
					So(err, ShouldBeNil)
					So(resp.Kvs, ShouldNotBeNil)
					So(len(resp.Kvs), ShouldBeGreaterThan, len(gateways)-numClients)
				}
			}

			// shutdown the cluster
			// send sigterm to each gateway
			for index, g := range gateways {
				fmt.Println("signaling server: ", *g.config.ServerName)
				g.signalChan <- syscall.SIGTERM
				val := <-mainErrChs[index]
				fmt.Println(*g.config.ServerName, "returned", val)

				// This is a little hacky but it lets us avoid writing a dedicated
				// test to get coverage for the runningLoop() case where etcdStopCh returns
				g.signalChan = make(chan os.Signal, 5)

				// if the etcd server is nil the next statement out side of this if will block forever
				if g.etcdServer == nil {
					close(g.signalChan)
				}
				fmt.Println(g.runningLoop(context.Background()))
			}

			return
		})
		Reset(func() {
			// remove test config files
			for _, filename := range configFiles {
				So(os.Remove(filename), ShouldBeNil)
			}

			// remove test etcd data directories
			for _, etcdDataDirPath := range etcdDataDirs {
				// remove the temp dir so we can recreate it
				So(os.RemoveAll(etcdDataDirPath), ShouldBeNil)
			}

			// cancel the test context
			if cancel != nil {
				cancel()
			}
		})
	})
}
