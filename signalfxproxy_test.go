package main

import (
	"github.com/cep21/gohelpers/a"
	"io/ioutil"
	"os"
	"testing"
)

func TestProxyPidWrite(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	a.ExpectNil(t, writePidFile(filename))
}

func TestProxyPidWriteError(t *testing.T) {
	a.ExpectNotNil(t, writePidFile("/root"))
}

func TestProxyInvalidConfig(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	ioutil.WriteFile(filename, []byte{}, os.FileMode(0666))
	proxyCommandLineConfiguration = proxyCommandLineConfigurationT{
		configFileName: filename,
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
	proxyCommandLineConfiguration = proxyCommandLineConfigurationT{
		configFileName: filename,
		stopChannel:    make(chan bool),
	}
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
	}()
	proxyCommandLineConfiguration.main()
}

func TestProxyListenerError(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ListenFrom":[{"Type":"carbon"}, {"Type":"carbon"}]}`), os.FileMode(0666))
	proxyCommandLineConfiguration = proxyCommandLineConfigurationT{
		configFileName: filename,
		stopChannel:    make(chan bool),
	}
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
	}()
	proxyCommandLineConfiguration.main()
}

func TestProxyForwardError(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ForwardTo":[{"Type":"carbon", "Host":"192.168.100.108", "Timeout": "1s"}]}`), os.FileMode(0666))
	proxyCommandLineConfiguration = proxyCommandLineConfigurationT{
		configFileName: filename,
		stopChannel:    make(chan bool),
	}
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
	}()
	proxyCommandLineConfiguration.main()
}

func TestProxyUnknownForwarder(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ForwardTo":[{"Type":"unknown"}]}`), os.FileMode(0666))
	proxyCommandLineConfiguration = proxyCommandLineConfigurationT{
		configFileName: filename,
		stopChannel:    make(chan bool),
	}
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
	}()
	proxyCommandLineConfiguration.main()
}

func TestProxyUnknownListener(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ListenFrom":[{"Type":"unknown"}]}`), os.FileMode(0666))
	proxyCommandLineConfiguration = proxyCommandLineConfigurationT{
		configFileName: filename,
		stopChannel:    make(chan bool),
	}
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
	}()
	proxyCommandLineConfiguration.main()
}
