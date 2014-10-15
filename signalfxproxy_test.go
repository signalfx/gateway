package main

import (
	"testing"
	"io/ioutil"
	"os"
	"github.com/cep21/gohelpers/a"
)


func TestProxyPidWrite(t *testing.T) {
	a.ExpectEquals(t, nil, writePidFile("/tmp/pidfile"), "Expect no error writing PID")
}

func TestProxyPidWriteError(t *testing.T) {
	a.ExpectNotEquals(t, nil, writePidFile("/root"), "Expect error writing PID")
}

func TestProxyInvalidConfig(t *testing.T) {
	filename := "/tmp/config.db.3"
	ioutil.WriteFile(filename, []byte{}, os.FileMode(0666))
	proxyCommandLineConfiguration = proxyCommandLineConfigurationT{
		configFileName: filename,
		stopChannel: make(chan bool),
	}
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
	}()
	proxyCommandLineConfiguration.main()
}

func TestProxyEmptyConfig(t *testing.T) {
	filename := "/tmp/config.db.2"
	ioutil.WriteFile(filename, []byte(`{}`), os.FileMode(0666))
	proxyCommandLineConfiguration.configFileName = filename
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
	}()
	main()
}

func TestProxyOkLoading(t *testing.T) {
	filename := "/tmp/config.db.1"
	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ForwardTo":[{"Type":"csv", "Filename":"/tmp/acsvfile"}], "ListenFrom":[{"Type":"carbon", "Port":"11616"}]}`), os.FileMode(0666))
//	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ForwardTo":[{"Type":"csv", "Filename":"/tmp/acsvfile"}]}`), os.FileMode(0666))
	proxyCommandLineConfiguration = proxyCommandLineConfigurationT{
		configFileName: filename,
		stopChannel: make(chan bool),
	}
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
	}()
	proxyCommandLineConfiguration.main()
}

func TestProxyListenerError(t *testing.T) {
	filename := "/tmp/config.db.10"
	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ListenFrom":[{"Type":"carbon"}, {"Type":"carbon"}]}`), os.FileMode(0666))
	proxyCommandLineConfiguration = proxyCommandLineConfigurationT{
		configFileName: filename,
		stopChannel: make(chan bool),
	}
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
	}()
	proxyCommandLineConfiguration.main()
}

func TestProxyForwardError(t *testing.T) {
	filename := "/tmp/config.db.11"
	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ForwardTo":[{"Type":"carbon", "Host":"192.168.100.108", "Timeout": "1s"}]}`), os.FileMode(0666))
	proxyCommandLineConfiguration = proxyCommandLineConfigurationT{
		configFileName: filename,
		stopChannel: make(chan bool),
	}
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
	}()
	proxyCommandLineConfiguration.main()
}

func TestProxyUnknownForwarder(t *testing.T) {
	filename := "/tmp/config.db.4"
	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ForwardTo":[{"Type":"unknown"}]}`), os.FileMode(0666))
	proxyCommandLineConfiguration = proxyCommandLineConfigurationT{
		configFileName: filename,
		stopChannel: make(chan bool),
	}
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
	}()
	proxyCommandLineConfiguration.main()
}

func TestProxyUnknownListener(t *testing.T) {
	filename := "/tmp/config.db.6"
	ioutil.WriteFile(filename, []byte(`{"StatsDelay": "1m", "ListenFrom":[{"Type":"unknown"}]}`), os.FileMode(0666))
	proxyCommandLineConfiguration = proxyCommandLineConfigurationT{
		configFileName: filename,
		stopChannel: make(chan bool),
	}
	go func() {
		proxyCommandLineConfiguration.stopChannel <- true
	}()
	proxyCommandLineConfiguration.main()
}
