package main

import (
	"flag"
	"github.com/golang/glog"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/forwarder"
	"github.com/signalfuse/signalfxproxy/listener"
	"github.com/signalfuse/signalfxproxy/stats"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
)

func writePidFile(pidFileName string) error {
	pid := os.Getpid()
	err := ioutil.WriteFile(pidFileName, []byte(strconv.FormatInt(int64(pid), 10)), os.FileMode(0644))
	if err != nil {
		return err
	}
	return nil
}

type proxyCommandLineConfigurationT struct {
	configFileName string
	pidFileName    string
	pprofaddr      string
	stopChannel    chan bool
}

var proxyCommandLineConfiguration proxyCommandLineConfigurationT

func init() {
	flag.StringVar(&proxyCommandLineConfiguration.configFileName, "configfile", "sf/sfdbproxy.conf", "Name of the db proxy configuration file")
	flag.StringVar(&proxyCommandLineConfiguration.pidFileName, "signalfxproxypid", "signalfxproxy.pid", "Name of the file to store the PID in")
	flag.StringVar(&proxyCommandLineConfiguration.pprofaddr, "pprofaddr", "", "Address to open pprof info on")
	proxyCommandLineConfiguration.stopChannel = make(chan bool)
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) main() {
	// TODO: Configure this on command line.  Needed because JSON decoding can get kinda slow.
	numProcs := runtime.NumCPU()
	runtime.GOMAXPROCS(numProcs)

	if proxyCommandLineConfiguration.pprofaddr != "" {
		go func() {
			glog.Infof("Opening pprof debug information on %s", proxyCommandLineConfiguration.pprofaddr)
			err := http.ListenAndServe(proxyCommandLineConfiguration.pprofaddr, nil)
			glog.Infof("Finished listening: %s", err)
		}()
	}
	writePidFile(proxyCommandLineConfiguration.pidFileName)
	glog.Infof("Looking for config file %s\n", proxyCommandLineConfiguration.configFileName)

	loadedConfig, err := config.LoadConfig(proxyCommandLineConfiguration.configFileName)
	if err != nil {
		glog.Errorf("Unable to load config: %s", err)
		return
	}
	glog.Infof("Config is %s\n", loadedConfig)
	allForwarders := []core.DatapointStreamingAPI{}
	allStatKeepers := []core.StatKeeper{}
	for _, forwardConfig := range loadedConfig.ForwardTo {
		loader, ok := forwarder.AllForwarderLoaders[forwardConfig.Type]
		if !ok {
			glog.Errorf("Unknown loader type: %s", forwardConfig.Type)
			return
		}
		forwarder, err := loader(forwardConfig)
		if err != nil {
			glog.Errorf("Unable to loader forwarder: %s", err)
			return
		}
		allForwarders = append(allForwarders, forwarder)
		allStatKeepers = append(allStatKeepers, forwarder)
	}
	multiplexer := forwarder.NewStreamingDatapointDemultiplexer(allForwarders)
	allStatKeepers = append(allStatKeepers, multiplexer)

	allListeners := make([]listener.DatapointListener, len(loadedConfig.ListenFrom))
	for _, forwardConfig := range loadedConfig.ListenFrom {
		loader, ok := listener.AllListenerLoaders[forwardConfig.Type]
		if !ok {
			glog.Errorf("Unknown loader type: %s", forwardConfig.Type)
			return
		}
		listener, err := loader(multiplexer, forwardConfig)
		if err != nil {
			glog.Errorf("Unable to loader listener: %s", err)
			return
		}
		allListeners = append(allListeners, listener)
		allStatKeepers = append(allStatKeepers, listener)
	}

	allStatKeepers = append(allStatKeepers, stats.NewGolangStatKeeper())

	if loadedConfig.StatsDelayDuration != nil && *loadedConfig.StatsDelayDuration != 0 {
		go core.DrainStatsThread(*loadedConfig.StatsDelayDuration, allForwarders, allStatKeepers, proxyCommandLineConfiguration.stopChannel)
	} else {
		glog.Infof("Skipping stat keeping")
	}

	glog.Infof("Setup done.  Blocking!\n")
	_ = <-proxyCommandLineConfiguration.stopChannel
}

func main() {
	flag.Parse()
	proxyCommandLineConfiguration.main()
}
