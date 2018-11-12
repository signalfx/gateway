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
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/protocol/carbon"
	"github.com/signalfx/metricproxy/protocol/signalfx"
	. "github.com/smartystreets/goconvey/convey"
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
		p := proxy{
			flags: proxyFlags{
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

func TestInvalidClusterOperation(t *testing.T) {
	failingTestRun(t, invalidClusterOpConfig, true, "", "unsupported cluster-op specified \"woohoo\"")
}

func TestForwarderName(t *testing.T) {
	Convey("Forwarder names", t, func() {
		So(forwarderName(&config.ForwardTo{Name: pointer.String("bob")}), ShouldEqual, "bob")
		So(forwarderName(&config.ForwardTo{Type: "atype"}), ShouldEqual, "atype")
	})
}

func TestProxy1(t *testing.T) {
	var cancelfunc context.CancelFunc
	Convey("a setup carbon proxy", t, func() {
		sendTo := dptest.NewBasicSink()
		var ctx context.Context
		ctx, cancelfunc = context.WithCancel(context.Background())
		var p *proxy
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
			p = &proxy{
				flags: proxyFlags{
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

		Convey("should proxy a carbon point", func() {
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

func startProxies(ctx context.Context, proxies []*proxy, logger *log.Hierarchy) ([]*proxy, []chan error) {
	ps := make([]*proxy, 0, len(proxies))
	mainDoneChans := make([]chan error, 0, len(proxies))
	gmp := &goMaxProcs{}
	for _, p := range proxies {
		p.gomaxprocs = gmp.Set
		p.logger = logger.CreateChild()
		p.etcdMgr.logger = logger.CreateChild()
	retry:
		mainDoneChan := make(chan error)
		go func(p *proxy, mainDoneChan chan error) {
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

func TestProxyCluster(t *testing.T) {
	Convey("a setup proxy cluster", t, func() {
		ctx, cancelfunc := context.WithCancel(context.Background())
		var ps []*proxy
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
			var proxies = make([]*proxy, 0, len(etcdConfigs))
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

				So(ioutil.WriteFile(filename, []byte(proxyConf), os.FileMode(0666)), ShouldBeNil)

				proxies = append(proxies, &proxy{
					stdout:          os.Stdout,
					tk:              timekeeper.RealTime{},
					setupDoneSignal: make(chan struct{}),
					signalChan:      make(chan os.Signal),
					flags: proxyFlags{
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
				{removeTimeout: 3000, ServerConfig: etcd.ServerConfig{UnhealthyMemberTTL: 1000 * time.Millisecond, DataDir: "etcd-data", LCAddress: "127.0.0.1:2379", ACAddress: "127.0.0.1:2379", LPAddress: "127.0.0.1:2380", APAddress: "127.0.0.1:2380", MAddress: "127.0.0.1:2381"}, operation: "seed"},
				{removeTimeout: 3000, ServerConfig: etcd.ServerConfig{UnhealthyMemberTTL: 1000 * time.Millisecond, DataDir: "etcd-data1", LCAddress: "127.0.0.1:2479", ACAddress: "127.0.0.1:2479", LPAddress: "127.0.0.1:2480", APAddress: "127.0.0.1:2480", MAddress: "127.0.0.1:2481"}, targetCluster: []string{"127.0.0.1:2379"}, operation: "join"},
				{removeTimeout: -1, ServerConfig: etcd.ServerConfig{UnhealthyMemberTTL: 1000 * time.Millisecond, DataDir: "etcd-data2", LCAddress: "127.0.0.1:2579", ACAddress: "127.0.0.1:2579", LPAddress: "127.0.0.1:2580", APAddress: "127.0.0.1:2580", MAddress: "127.0.0.1:2581"}, targetCluster: []string{"127.0.0.1:2379", "127.0.0.1:2479"}, operation: "join"},
				{removeTimeout: 3000, ServerConfig: etcd.ServerConfig{UnhealthyMemberTTL: 1000 * time.Millisecond, DataDir: "etcd-data3", LCAddress: "127.0.0.1:2679", ACAddress: "127.0.0.1:2679", LPAddress: "127.0.0.1:2680", APAddress: "127.0.0.1:2680", MAddress: "127.0.0.1:2681"}, targetCluster: []string{"127.0.0.1:2379", "127.0.0.1:2479", "127.0.0.1:2579"}, operation: "join"},
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
