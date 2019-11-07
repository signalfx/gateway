package csv

import (
	"io/ioutil"
	"os"
	"testing"

	"errors"

	"context"
	"net/http"

	"github.com/signalfx/gateway/protocol/filtering"
	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/datapoint/dptest"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/pointer"
	"github.com/signalfx/golib/v3/trace"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestFilenameForwarder(t *testing.T) {
	ctx := context.Background()
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer func() {
		assert.NoError(t, os.Remove(filename))
	}()
	conf := &Config{
		Filename: pointer.String(fileObj.Name()),
	}
	f, err := NewForwarder(conf)
	defer func() {
		assert.NoError(t, f.Close())
	}()
	assert.NoError(t, err)
	assert.Equal(t, len(f.Datapoints()), 1)
	assert.NoError(t, f.AddDatapoints(ctx, []*datapoint.Datapoint{dptest.DP()}))
	assert.NoError(t, f.AddEvents(ctx, []*event.Event{dptest.E()}))
	assert.NoError(t, f.AddSpans(ctx, []*trace.Span{{}}))
	assert.Equal(t, int64(0), f.Pipeline())
	assert.Nil(t, f.StartupFinished())
	assert.Equal(t, f.DebugEndpoints(), map[string]http.Handler{})
}

func TestFilenameForwarderBadFilename(t *testing.T) {
	conf := &Config{
		Filename: pointer.String("/"),
	}
	_, err := NewForwarder(conf)
	assert.Error(t, err)
}

func TestFilenameForwarderBadWrite(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	defer func() {
		assert.NoError(t, os.Remove(fileObj.Name()))
	}()
	conf := &Config{
		Filename: pointer.String(fileObj.Name()),
		WriteString: func(f *os.File, s string) (ret int, err error) {
			return 0, errors.New("nope")
		},
	}
	f, _ := NewForwarder(conf)
	ctx := context.Background()
	assert.Error(t, f.AddDatapoints(ctx, []*datapoint.Datapoint{dptest.DP()}))
	assert.Error(t, f.AddEvents(ctx, []*event.Event{dptest.E()}))
	assert.Error(t, f.AddSpans(ctx, []*trace.Span{{}}))
}

func TestBadForwarderCOnfig(t *testing.T) {
	Convey("Invalid regexes should cause an error", t, func() {
		fileObj, _ := ioutil.TempFile("", "gotest")
		defer func() {
			assert.NoError(t, os.Remove(fileObj.Name()))
		}()
		forwardConfig := &Config{
			Filename: pointer.String(fileObj.Name()),
			Filters: &filtering.FilterObj{
				Allow: []string{"["},
			},
		}
		forwarder, err := NewForwarder(forwardConfig)
		So(err, ShouldNotBeNil)
		So(forwarder, ShouldBeNil)
	})
}
