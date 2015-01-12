package main

import (
	"bufio"
	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"testing"
)

var config1 = `
  {
    "StatsDelay": "1m",
    "ListenFrom":[
      {
      	"Type":"carbon",
      	"ListenAddr": "0.0.0.0:0"
	  }
    ],
    "ForwardTo":[
      {
      	"Type":"carbon",
      	"Host":"0.0.0.0",
      	"DimensionsOrder": ["source", "forwarder"],
      	"Name": "testForwardTo",
      	"Port": <<PORT>>
      }
    ]
  }
`

func TestProxyPidWrite(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	assert.Nil(t, writePidFile(filename))
}

func TestProxyPidWriteError(t *testing.T) {
	assert.Error(t, writePidFile("/root"))
}

func TestConfigLoadDimensions(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	psocket, err := net.Listen("tcp", "0.0.0.0:0")
	assert.NoError(t, err)
	defer psocket.Close()
	portParts := strings.Split(psocket.Addr().String(), ":")
	port := portParts[len(portParts)-1]
	conf := strings.Replace(config1, "<<PORT>>", port, 1)

	ioutil.WriteFile(filename, []byte(conf), os.FileMode(0666))
	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
		configFileName:                filename,
		logDir:                        os.TempDir(),
		logMaxSize:                    1,
		logMaxBackups:                 0,
		stopChannel:                   make(chan bool),
		closeWhenWaitingToStopChannel: make(chan struct{}),
	}

	go func() {
		myProxyCommandLineConfiguration.blockTillSetupReady()
		assert.Equal(t, 1, len(myProxyCommandLineConfiguration.allListeners))
		assert.Equal(t, 1, len(myProxyCommandLineConfiguration.allForwarders))
		myProxyCommandLineConfiguration.statDrainThread.SendStats()
		c, err := psocket.Accept()
		defer c.Close()
		assert.NoError(t, err)
		reader := bufio.NewReader(c)
		line, err := reader.ReadString((byte)('\n'))
		assert.NoError(t, err)
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
	proxyCommandLineConfiguration = proxyCommandLineConfigurationT{
		configFileName: filename,
		logDir:         os.TempDir(),
		logMaxSize:     1,
		logMaxBackups:  0,
		stopChannel:    make(chan bool),
	}
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
	}()
	proxyCommandLineConfiguration.main()
}

func TestProxyEmptyConfig(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	ioutil.WriteFile(filename, []byte(`{}`), os.FileMode(0666))
	proxyCommandLineConfiguration.configFileName = filename
	proxyCommandLineConfiguration.pprofaddr = "0.0.0.0:0"
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
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
	_, ok := myProxyCommandLineConfiguration.getLogrusFormatter().(*log.JSONFormatter)
	assert.True(t, ok)
}

func TestGetLogrusOutput(t *testing.T) {
	myProxyCommandLineConfiguration := proxyCommandLineConfigurationT{
		logDir: "-",
	}
	assert.Equal(t, os.Stdout, myProxyCommandLineConfiguration.getLogrusOutput())
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
	}
	go func() {
		myProxyCommandLineConfiguration.stopChannel <- true
	}()
	myProxyCommandLineConfiguration.main()
}
