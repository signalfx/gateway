package main

import (
	"bufio"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"testing"

	"fmt"
	"io"

	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/metricproxy/config"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

var config1 = `
  {
    "StatsDelay": "1m",
    "NumProcs": 1,
    "LogDir": "sfproxytest.json",
    "LogMaxSize": 5,
    "LogLevel": "info",
    "PidFilename": "metricproxy.pid",
    "LogMaxBackups": 5,
    "LogFormat": "stdout",
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
      	"DimensionsOrder": ["source", "forwarder"],
      	"Name": "testForwardTo",
      	"Port": <<PORT>>
      }
    ]
  }
`

func init() {
	flagParse = func() {
		// Don't parse flags while testing main()
	}
}

func TestProxyPidWrite(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	assert.Nil(t, writePidFile(filename))
}

func TestInvalidLogLevel(t *testing.T) {
	assert.Panics(t, func() {
		logLevelMustParse("invalid_log_level")
	})
}

func TestProxyPidWriteError(t *testing.T) {
	assert.Error(t, writePidFile("/root"))
}

func TestConfigLoadDimensions(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)
	ctx := context.Background()

	psocket, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer psocket.Close()
	portParts := strings.Split(psocket.Addr().String(), ":")
	port := portParts[len(portParts)-1]
	conf := strings.Replace(config1, "<<PORT>>", port, 1)

	ioutil.WriteFile(filename, []byte(conf), os.FileMode(0666))
	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
		configFileName:                filename,
		logDir:                        "-",
		logMaxSize:                    1,
		ctx:                           ctx,
		logMaxBackups:                 0,
		stopChannel:                   make(chan bool),
		closeWhenWaitingToStopChannel: make(chan struct{}),
	}

	go func() {
		myProxyCommandLineConfiguration.blockTillSetupReady()
		assert.Equal(t, 1, len(myProxyCommandLineConfiguration.allListeners))
		assert.Equal(t, 1, len(myProxyCommandLineConfiguration.allForwarders))
		dp := datapoint.New("metric", map[string]string{"source": "proxy", "forwarder": "testForwardTo"}, datapoint.NewIntValue(1), datapoint.Gauge, time.Now())
		myProxyCommandLineConfiguration.allForwarders[0].AddDatapoints(ctx, []*datapoint.Datapoint{dp})
		// Keep going, but skip empty line and EOF
		line := ""
		for {
			c, err := psocket.Accept()
			defer c.Close()
			assert.NoError(t, err)
			reader := bufio.NewReader(c)
			line, err = reader.ReadString((byte)('\n'))
			if line == "" && err == io.EOF {
				continue
			}
			break
		}
		assert.NoError(t, err)
		fmt.Printf("line is %s\n", line)
		log.Info(line)
		assert.Equal(t, "proxy.testForwardTo.", line[0:len("proxy.testForwardTo.")])
		myProxyCommandLineConfiguration.stopChannel <- true
	}()
	assert.NoError(t, myProxyCommandLineConfiguration.main())
}

func TestProxyInvalidConfig(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	ioutil.WriteFile(filename, []byte{}, os.FileMode(0666))
	proxyCommandLineConfigurationDefault = proxyCommandLineConfigurationT{
		configFileName: filename,
		logDir:         os.TempDir(),
		logMaxSize:     1,
		logMaxBackups:  0,
		stopChannel:    make(chan bool),
		ctx:            context.Background(),
	}
	go func() {
		proxyCommandLineConfigurationDefault.stopChannel <- true
	}()
	proxyCommandLineConfigurationDefault.main()
}

func TestProxyEmptyConfig(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	ioutil.WriteFile(filename, []byte(`{}`), os.FileMode(0666))
	proxyCommandLineConfigurationDefault.configFileName = filename
	proxyCommandLineConfigurationDefault.pprofaddr = "localhost:99999"
	go func() {
		proxyCommandLineConfigurationDefault.stopChannel <- true
	}()
	main()
}

func TestProxyOkLoading(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)
	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ForwardTo":[{"Type":"csv", "Filename":"/tmp/acsvfile"}], "ListenFrom":[{"Type":"carbon", "Port":"11616"}]}`), os.FileMode(0666))
	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
		configFileName: filename,
		logDir:         os.TempDir(),
		logMaxSize:     1,
		logMaxBackups:  0,
		stopChannel:    make(chan bool),
		ctx:            context.Background(),
	}
	go func() {
		myProxyCommandLineConfiguration.stopChannel <- true
	}()
	myProxyCommandLineConfiguration.main()
}

func TestProxyListenerError(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ListenFrom":[{"Type":"carbon"}, {"Type":"carbon"}]}`), os.FileMode(0666))
	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
		configFileName: filename,
		logDir:         os.TempDir(),
		logMaxSize:     1,
		logMaxBackups:  0,
		stopChannel:    make(chan bool),
		ctx:            context.Background(),
	}
	go func() {
		myProxyCommandLineConfiguration.stopChannel <- true
	}()
	myProxyCommandLineConfiguration.main()
}

func TestProxyForwardError(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ForwardTo":[{"Type":"carbon", "Host":"192.168.100.108", "Timeout": "1ms"}]}`), os.FileMode(0666))
	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
		configFileName: filename,
		logDir:         os.TempDir(),
		logMaxSize:     1,
		logMaxBackups:  0,
		stopChannel:    make(chan bool),
		ctx:            context.Background(),
	}
	go func() {
		myProxyCommandLineConfiguration.stopChannel <- true
	}()
	myProxyCommandLineConfiguration.main()
}

func TestGetLogrusFormatter(t *testing.T) {
	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
		logJSON: true,
	}
	_, ok := myProxyCommandLineConfiguration.getLogrusFormatter(&config.ProxyConfig{}).(*log.JSONFormatter)
	assert.True(t, ok)
}

func TestGetLogrusOutput(t *testing.T) {
	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
		logDir: "-",
	}
	assert.Equal(t, os.Stdout, myProxyCommandLineConfiguration.getLogrusOutput(&config.ProxyConfig{}))
}

func TestProxyUnknownForwarder(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ForwardTo":[{"Type":"unknown"}]}`), os.FileMode(0666))
	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
		configFileName: filename,
		logDir:         os.TempDir(),
		logMaxSize:     1,
		logMaxBackups:  0,
		stopChannel:    make(chan bool),
		ctx:            context.Background(),
	}
	go func() {
		myProxyCommandLineConfiguration.stopChannel <- true
	}()
	myProxyCommandLineConfiguration.main()
}

func TestProxyUnknownListener(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ListenFrom":[{"Type":"unknown"}]}`), os.FileMode(0666))
	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
		configFileName: filename,
		logDir:         os.TempDir(),
		logMaxSize:     1,
		logMaxBackups:  0,
		stopChannel:    make(chan bool),
		ctx:            context.Background(),
	}
	go func() {
		myProxyCommandLineConfiguration.stopChannel <- true
	}()
	myProxyCommandLineConfiguration.main()
}

func TestAllListeners(t *testing.T) {
	for _, l := range allListenerLoaders {
		assert.Panics(t, func() {
			l(nil, nil, nil)
		})
	}
}
