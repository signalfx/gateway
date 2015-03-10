package csv

import (
	"io/ioutil"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/stats"
)

func originalFileWrite(f *os.File, str string) (int, error) {
	return f.WriteString(str)
}

var fileXXXWriteString = originalFileWrite

var csvDefaultConfig = &config.ForwardTo{
	Filename:        workarounds.GolangDoesnotAllowPointerToStringLiteral("datapoints.csv"),
	DrainingThreads: workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(1)),
	Name:            workarounds.GolangDoesnotAllowPointerToStringLiteral("filename-drainer"),
	MaxDrainSize:    workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(100)),
	BufferSize:      workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(100)),
}

// ForwarderLoader loads a CSV forwarder forwarding points from proxy to a file
func ForwarderLoader(forwardTo *config.ForwardTo) (stats.StatKeepingStreamer, error) {
	structdefaults.FillDefaultFrom(forwardTo, csvDefaultConfig)
	log.WithField("forwardTo", forwardTo).Info("Creating CSV using final config")
	return NewForwarder(*forwardTo.BufferSize, *forwardTo.Name, *forwardTo.Filename, *forwardTo.MaxDrainSize)
}

type filenameForwarder struct {
	datapoint.BufferedForwarder
	filename string
}

func (connector *filenameForwarder) Stats() []datapoint.Datapoint {
	ret := []datapoint.Datapoint{}
	return ret
}

func (connector *filenameForwarder) process(datapoints []datapoint.Datapoint) error {
	file, err := os.OpenFile(connector.filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0666))
	if err != nil {
		return err
	}
	defer file.Sync()
	defer file.Close()
	for _, dp := range datapoints {
		_, err := fileXXXWriteString(file, dp.String()+"\n")
		if err != nil {
			return err
		}
	}
	return nil
}

var osXXXRemove = os.Remove

// NewForwarder creates a new CSV forwarder
func NewForwarder(bufferSize uint32, name string, filename string, maxDrainSize uint32) (stats.StatKeepingStreamer, error) {
	ret := &filenameForwarder{
		BufferedForwarder: *datapoint.NewBufferedForwarder(bufferSize, maxDrainSize, name, uint32(1)),
		filename:          filename,
	}
	err := osXXXRemove(filename)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err := ioutil.WriteFile(filename, []byte{}, os.FileMode(0666)); err != nil {
		log.WithField("filename", filename).Info("Unable to verify write for file")
		return nil, err
	}
	ret.Start(ret.process)
	return ret, nil
}
