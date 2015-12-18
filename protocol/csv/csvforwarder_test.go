package csv

import (
	"io/ioutil"
	"os"
	"testing"

	"errors"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestFilenameForwarder(t *testing.T) {
	ctx := context.Background()
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)
	conf := &config.ForwardTo{
		Filename: workarounds.GolangDoesnotAllowPointerToStringLiteral(fileObj.Name()),
	}
	f, err := ForwarderLoader(conf)
	defer f.Close()
	assert.NoError(t, err)
	assert.NoError(t, f.AddDatapoints(ctx, []*datapoint.Datapoint{dptest.DP()}))
	assert.NoError(t, f.AddEvents(ctx, []*event.Event{dptest.E()}))
	assert.Equal(t, 0, len(f.Stats()))
}

func TestFilenameForwarderBadFilename(t *testing.T) {
	_, err := NewForwarder("abcd", "/")
	assert.Error(t, err)
}

func TestFilenameForwarderBadOpen(t *testing.T) {
	ctx := context.Background()
	fileObj, _ := ioutil.TempFile("", "gotest")
	defer os.Remove(fileObj.Name())

	f, _ := NewForwarder("unused", fileObj.Name())

	f.filename = "/"
	assert.Error(t, f.AddDatapoints(ctx, []*datapoint.Datapoint{}))
	assert.Error(t, f.AddEvents(ctx, []*event.Event{}))
}

func TestFilenameForwarderBadWrite(t *testing.T) {
	ctx := context.Background()
	fileObj, _ := ioutil.TempFile("", "gotest")
	defer os.Remove(fileObj.Name())
	conf := &config.ForwardTo{
		Filename: workarounds.GolangDoesnotAllowPointerToStringLiteral(fileObj.Name()),
	}
	f, _ := ForwarderLoader(conf)
	f.writeString = func(f *os.File, s string) (ret int, err error) {
		return 0, errors.New("nope")
	}
	assert.Error(t, f.AddDatapoints(ctx, []*datapoint.Datapoint{dptest.DP()}))
	assert.Error(t, f.AddEvents(ctx, []*event.Event{dptest.E()}))
}
