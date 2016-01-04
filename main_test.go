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

//import (
//	"bufio"
//	"io/ioutil"
//	"net"
//	"os"
//	"strings"
//	"testing"
//
//	"fmt"
//	"io"
//
//	"time"
//
//	"github.com/signalfx/golib/datapoint"
//	"github.com/signalfx/golib/log"
//	"github.com/signalfx/metricproxy/config"
//	"github.com/stretchr/testify/assert"
//	"golang.org/x/net/context"
//)

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
		fileObj, _ := ioutil.TempFile("", "gotest")
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

//
//func init() {
//	flagParse = func() {
//		// Don't parse flags while testing main()
//	}
//}
//
//func TestProxyPidWrite(t *testing.T) {
//	fileObj, _ := ioutil.TempFile("", "gotest")
//	filename := fileObj.Name()
//	defer os.Remove(filename)
//
//	assert.Nil(t, writePidFile(filename))
//}
//
//func TestProxyPidWriteError(t *testing.T) {
//	assert.Error(t, writePidFile("/root"))
//}
//
//func TestGetLogOutput(t *testing.T) {
//	x := proxyCommandLineConfigurationT{
//		logDir:  "-",
//		logJSON: true,
//		logger:  log.Discard,
//	}
//	assert.Equal(t, os.Stdout, x.getLogOutput(&config.ProxyConfig{}))
//
//	l := x.getLogger(&config.ProxyConfig{})
//	_, ok := l.(*log.ErrorLogLogger).RootLogger.(*log.JSONLogger)
//	assert.True(t, ok)
//}
//
//func TestConfigLoadDimensions(t *testing.T) {
//	fileObj, _ := ioutil.TempFile("", "gotest")
//	filename := fileObj.Name()
//	defer os.Remove(filename)
//	ctx := context.Background()
//
//	psocket, err := net.Listen("tcp", "127.0.0.1:0")
//	assert.NoError(t, err)
//	defer psocket.Close()
//	portParts := strings.Split(psocket.Addr().String(), ":")
//	port := portParts[len(portParts)-1]
//	conf := strings.Replace(config1, "<<PORT>>", port, 1)
//
//	ioutil.WriteFile(filename, []byte(conf), os.FileMode(0666))
//	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
//		configFileName:                filename,
//		logDir:                        "-",
//		logMaxSize:                    1,
//		ctx:                           ctx,
//		logger:                        log.Discard,
//		logMaxBackups:                 0,
//		stopChannel:                   make(chan bool),
//		closeWhenWaitingToStopChannel: make(chan struct{}),
//	}
//
//	go func() {
//		myProxyCommandLineConfiguration.blockTillSetupReady()
//		assert.Equal(t, 1, len(myProxyCommandLineConfiguration.allListeners))
//		assert.Equal(t, 1, len(myProxyCommandLineConfiguration.allForwarders))
//		dp := datapoint.New("metric", map[string]string{"source": "proxy", "forwarder": "testForwardTo"}, datapoint.NewIntValue(1), datapoint.Gauge, time.Now())
//		myProxyCommandLineConfiguration.allForwarders[0].AddDatapoints(ctx, []*datapoint.Datapoint{dp})
//		// Keep going, but skip empty line and EOF
//		line := ""
//		for {
//			c, err := psocket.Accept()
//			defer c.Close()
//			assert.NoError(t, err)
//			reader := bufio.NewReader(c)
//			line, err = reader.ReadString((byte)('\n'))
//			if line == "" && err == io.EOF {
//				continue
//			}
//			break
//		}
//		assert.NoError(t, err)
//		fmt.Printf("line is %s\n", line)
//		assert.Equal(t, "proxy.testForwardTo.", line[0:len("proxy.testForwardTo.")])
//		myProxyCommandLineConfiguration.stopChannel <- true
//	}()
//	assert.NoError(t, myProxyCommandLineConfiguration.main())
//}
//
//func TestProxyInvalidConfig(t *testing.T) {
//	fileObj, _ := ioutil.TempFile("", "gotest")
//	filename := fileObj.Name()
//	defer os.Remove(filename)
//
//	ioutil.WriteFile(filename, []byte{}, os.FileMode(0666))
//	proxyCommandLineConfigurationDefault = proxyCommandLineConfigurationT{
//		configFileName: filename,
//		logDir:         os.TempDir(),
//		logMaxSize:     1,
//		logMaxBackups:  0,
//		stopChannel:    make(chan bool),
//		logger:         log.Discard,
//		ctx:            context.Background(),
//	}
//	go func() {
//		proxyCommandLineConfigurationDefault.stopChannel <- true
//	}()
//	proxyCommandLineConfigurationDefault.main()
//}
//
//func TestProxyEmptyConfig(t *testing.T) {
//	fileObj, _ := ioutil.TempFile("", "gotest")
//	filename := fileObj.Name()
//	defer os.Remove(filename)
//
//	ioutil.WriteFile(filename, []byte(`{}`), os.FileMode(0666))
//	proxyCommandLineConfigurationDefault.configFileName = filename
//	proxyCommandLineConfigurationDefault.pprofaddr = "localhost:99999"
//	go func() {
//		proxyCommandLineConfigurationDefault.stopChannel <- true
//	}()
//	main()
//}
//
//func TestProxyOkLoading(t *testing.T) {
//	fileObj, _ := ioutil.TempFile("", "gotest")
//	filename := fileObj.Name()
//	defer os.Remove(filename)
//	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ForwardTo":[{"Type":"csv", "Filename":"/tmp/acsvfile"}], "ListenFrom":[{"Type":"carbon", "Port":"11616"}]}`), os.FileMode(0666))
//	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
//		configFileName: filename,
//		logDir:         os.TempDir(),
//		logMaxSize:     1,
//		logMaxBackups:  0,
//		stopChannel:    make(chan bool),
//		ctx:            context.Background(),
//		logger:         log.Discard,
//	}
//	go func() {
//		myProxyCommandLineConfiguration.stopChannel <- true
//	}()
//	myProxyCommandLineConfiguration.main()
//}
//
//func TestProxyListenerError(t *testing.T) {
//	fileObj, _ := ioutil.TempFile("", "gotest")
//	filename := fileObj.Name()
//	defer os.Remove(filename)
//
//	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ListenFrom":[{"Type":"carbon"}, {"Type":"carbon"}]}`), os.FileMode(0666))
//	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
//		configFileName: filename,
//		logDir:         os.TempDir(),
//		logMaxSize:     1,
//		logMaxBackups:  0,
//		stopChannel:    make(chan bool),
//		ctx:            context.Background(),
//		logger:         log.Discard,
//	}
//	go func() {
//		myProxyCommandLineConfiguration.stopChannel <- true
//	}()
//	myProxyCommandLineConfiguration.main()
//}
//
//func TestProxyForwardError(t *testing.T) {
//	fileObj, _ := ioutil.TempFile("", "gotest")
//	filename := fileObj.Name()
//	defer os.Remove(filename)
//
//	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ForwardTo":[{"Type":"carbon", "Host":"192.168.100.108", "Timeout": "1ms"}]}`), os.FileMode(0666))
//	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
//		configFileName: filename,
//		logDir:         os.TempDir(),
//		logMaxSize:     1,
//		logMaxBackups:  0,
//		stopChannel:    make(chan bool),
//		ctx:            context.Background(),
//		logger:         log.Discard,
//	}
//	go func() {
//		myProxyCommandLineConfiguration.stopChannel <- true
//	}()
//	myProxyCommandLineConfiguration.main()
//}
//
//func TestProxyUnknownForwarder(t *testing.T) {
//	fileObj, _ := ioutil.TempFile("", "gotest")
//	filename := fileObj.Name()
//	defer os.Remove(filename)
//
//	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ForwardTo":[{"Type":"unknown"}]}`), os.FileMode(0666))
//	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
//		configFileName: filename,
//		logDir:         os.TempDir(),
//		logMaxSize:     1,
//		logMaxBackups:  0,
//		stopChannel:    make(chan bool),
//		ctx:            context.Background(),
//		logger:         log.Discard,
//	}
//	go func() {
//		myProxyCommandLineConfiguration.stopChannel <- true
//	}()
//	myProxyCommandLineConfiguration.main()
//}
//
//func TestProxyUnknownListener(t *testing.T) {
//	fileObj, _ := ioutil.TempFile("", "gotest")
//	filename := fileObj.Name()
//	defer os.Remove(filename)
//
//	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ListenFrom":[{"Type":"unknown"}]}`), os.FileMode(0666))
//	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
//		configFileName: filename,
//		logDir:         os.TempDir(),
//		logMaxSize:     1,
//		logMaxBackups:  0,
//		stopChannel:    make(chan bool),
//		ctx:            context.Background(),
//		logger:         log.Discard,
//	}
//	go func() {
//		myProxyCommandLineConfiguration.stopChannel <- true
//	}()
//	myProxyCommandLineConfiguration.main()
//}
//
//func TestAllListeners(t *testing.T) {
//	for _, l := range allListenerLoaders {
//		assert.Panics(t, func() {
//			l(nil, nil, nil, nil)
//		})
//	}
//}
