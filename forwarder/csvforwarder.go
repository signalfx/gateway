package forwarder

import (
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/golang/glog"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"os"
)

var csvDefaultConfig = &config.ForwardTo{
	Filename:        workarounds.GolangDoesnotAllowPointerToStringLiteral("https://api.signalfuse.com/v1/datapoint"),
	BufferSize:      workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(10000)),
	DrainingThreads: workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(1)),
	Name:            workarounds.GolangDoesnotAllowPointerToStringLiteral("filename-drainer"),
	MaxDrainSize:    workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(100)),
}

// CsvForwarderLoader loads a CSV forwarder forwarding points from proxy to a file
func CsvForwarderLoader(forwardTo *config.ForwardTo) (core.StatKeepingStreamingAPI, error) {
	structdefaults.FillDefaultFrom(forwardTo, csvDefaultConfig)
	glog.Infof("Creating CSV using final config %s", forwardTo)
	return NewCsvForwarder(*forwardTo.BufferSize, *forwardTo.Name, *forwardTo.Filename, *forwardTo.MaxDrainSize)
}

type filenameForwarder struct {
	*basicBufferedForwarder
	filename string
}

func (connector *filenameForwarder) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
	return ret
}

func (connector *filenameForwarder) process(datapoints []core.Datapoint) error {
	file, err := os.OpenFile(connector.filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0666))
	if err != nil {
		return err
	}
	defer file.Sync()
	defer file.Close()
	for _, dp := range datapoints {
		_, err := file.WriteString(dp.String() + "\n")
		if err != nil {
			return err
		}
	}
	return nil
}

// NewCsvForwarder creates a new CSV forwarder
func NewCsvForwarder(bufferSize uint32, name string, filename string, maxDrainSize uint32) (core.StatKeepingStreamingAPI, error) {
	ret := &filenameForwarder{
		basicBufferedForwarder: NewBasicBufferedForwarder(bufferSize, maxDrainSize, name, uint32(1)),
		filename:               filename,
	}
	os.Remove(filename)
	ret.start(ret.process)
	return ret, nil
}
