package forwarder

import (
	"errors"
	"github.com/cep21/gohelpers/a"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
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

func TestCsvForwarderLoader(t *testing.T) {
	defer func() { os.Remove("datapoints.csv") }()
	forwardTo := config.ForwardTo{
		Filename: workarounds.GolangDoesnotAllowPointerToStringLiteral("/tmp/tempfile.csv"),
	}
	cl, err := CsvForwarderLoader(&forwardTo)
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 0, len(cl.GetStats()), "Expect no stats")

	dpSent := core.NewRelativeTimeDatapoint("metric", map[string]string{}, value.NewIntWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	cl.DatapointsChannel() <- dpSent
}

func TestCsvInvalidFilenameCsvForwarderLoader(t *testing.T) {

	defer func() { os.Remove("datapoints.csv") }()
	forwardTo := config.ForwardTo{
		Filename: workarounds.GolangDoesnotAllowPointerToStringLiteral("/root"),
	}
	osXXXRemove = func(s string) error { return nil }
	_, err := CsvForwarderLoader(&forwardTo)
	osXXXRemove = os.Remove

	assert.NotEqual(t, nil, err, "Expect no error")
}

func TestCsvInvalidOpen(t *testing.T) {
	defer func() { os.Remove("datapoints.csv") }()
	forwardTo := config.ForwardTo{}
	cl, err := CsvForwarderLoader(&forwardTo)
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 0, len(cl.GetStats()), "Expect no stats")

	dpSent := core.NewRelativeTimeDatapoint("metric", map[string]string{}, value.NewIntWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	clOrig, ok := cl.(*filenameForwarder)
	assert.Equal(t, true, ok, "Expect no error")
	clOrig.filename = "/root"
	cl.DatapointsChannel() <- dpSent
}

func TestCsvRemoveError(t *testing.T) {
	defer func() { os.Remove("datapoints.csv") }()
	forwardTo := config.ForwardTo{}
	osXXXRemove = func(s string) error { return errors.New("unable to remove") }
	defer func() { osXXXRemove = os.Remove }()
	_, err := CsvForwarderLoader(&forwardTo)
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
	cl, _ := CsvForwarderLoader(&forwardTo)
	dpSent := core.NewRelativeTimeDatapoint("metric", map[string]string{}, value.NewIntWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	cl.DatapointsChannel() <- dpSent
	_ = <-c
	// Expect this chan to be eventually drained
}
