package csv

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/cep21/gohelpers/a"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/stretchr/testify/assert"
)

var fileStub a.FileWriteStringObj

func init() {
	fileXXXWriteString = fileStub.Execute
}

func TestCsvCoverFileWrite(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)
	_, err := originalFileWrite(fileObj, "test")
	assert.Nil(t, err)
}

func TestForwarderLoader(t *testing.T) {
	defer func() { os.Remove("datapoints.csv") }()
	forwardTo := config.ForwardTo{
		Filename: workarounds.GolangDoesnotAllowPointerToStringLiteral("/tmp/tempfile.csv"),
	}
	cl, err := ForwarderLoader(&forwardTo)
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 0, len(cl.Stats()), "Expect no stats")

	dpSent := datapoint.NewRelativeTime("metric", map[string]string{}, datapoint.NewIntValue(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	cl.Channel() <- dpSent
}

func TestCsvInvalidFilenameForwarderLoader(t *testing.T) {

	defer func() { os.Remove("datapoints.csv") }()
	forwardTo := config.ForwardTo{
		Filename: workarounds.GolangDoesnotAllowPointerToStringLiteral("/root"),
	}
	osXXXRemove = func(s string) error { return nil }
	_, err := ForwarderLoader(&forwardTo)
	osXXXRemove = os.Remove

	assert.NotEqual(t, nil, err, "Expect no error")
}

func TestCsvInvalidOpen(t *testing.T) {
	defer func() { os.Remove("datapoints.csv") }()
	forwardTo := config.ForwardTo{}
	cl, err := ForwarderLoader(&forwardTo)
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 0, len(cl.Stats()), "Expect no stats")

	dpSent := datapoint.NewRelativeTime("metric", map[string]string{}, datapoint.NewIntValue(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	clOrig, ok := cl.(*filenameForwarder)
	assert.Equal(t, true, ok, "Expect no error")
	clOrig.filename = "/root"
	cl.Channel() <- dpSent
}

func TestCsvRemoveError(t *testing.T) {
	defer func() { os.Remove("datapoints.csv") }()
	forwardTo := config.ForwardTo{}
	osXXXRemove = func(s string) error { return errors.New("unable to remove") }
	defer func() { osXXXRemove = os.Remove }()
	_, err := ForwarderLoader(&forwardTo)
	assert.NotEqual(t, nil, err, "Expect remove error")
}

func TestCsvWriteError(t *testing.T) {
	defer func() { os.Remove("datapoints.csv") }()
	forwardTo := config.ForwardTo{}
	c := make(chan bool)
	fileStub.UseFunction(func(f *os.File, str string) (int, error) {
		defer func() { c <- true }()
		return 0, errors.New("an error")
	})
	defer fileStub.Reset()
	cl, _ := ForwarderLoader(&forwardTo)
	dpSent := datapoint.NewRelativeTime("metric", map[string]string{}, datapoint.NewIntValue(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	cl.Channel() <- dpSent
	_ = <-c
	// Expect this chan to be eventually drained
}
