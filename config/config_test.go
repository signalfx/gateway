package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/signalfx/golib/log"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestInvalidConfig(t *testing.T) {
	_, err := Load("invalidodesnotexist___SDFSDFSD", log.Discard)
	assert.Error(t, err)
}

func TestStringConv(t *testing.T) {
	name := string("aname")
	lf := ListenFrom{Name: &name}
	assert.Contains(t, lf.String(), "aname")

	ft := ForwardTo{Name: &name}
	assert.Contains(t, ft.String(), "aname")
}

func TestBadDecode(t *testing.T) {
	_, err := decodeConfig([]byte("badjson"))
	assert.Error(t, err)
}

func TestGetDefaultName(t *testing.T) {
	u := getDefaultName(func() (string, error) {
		return "", errors.New("")
	})
	assert.Equal(t, u, "unknown")

}

func TestParseStatsDelay(t *testing.T) {
	config, _ := decodeConfig([]byte(`{"StatsDelay":"3s"}`))
	assert.Equal(t, *config.StatsDelayDuration, time.Second*3)
	_, err := decodeConfig([]byte(`{"StatsDelay":"3r"}`))
	assert.Error(t, err)
}

func TestParseForwardTimeout(t *testing.T) {
	config, err := decodeConfig([]byte(`{"ForwardTo":[{"Timeout":"3s"}]}`))
	assert.Nil(t, err)
	assert.Equal(t, *config.ForwardTo[0].TimeoutDuration, time.Second*3)
	config, err = decodeConfig([]byte(`{"ForwardTo":[{"Timeout":"3r"}]}`))
	assert.Error(t, err)
}

func TestParseForwardOrdering(t *testing.T) {
	config, err := decodeConfig([]byte(`{"ForwardTo":[{"Timeout":"3s", "DimensionsOrder": ["hi"]}]}`))
	assert.Nil(t, err)
	assert.Equal(t, *config.ForwardTo[0].TimeoutDuration, time.Second*3)
	assert.Equal(t, config.ForwardTo[0].DimensionsOrder[0], "hi")
	assert.Equal(t, 1, len(config.ForwardTo[0].DimensionsOrder))
	assert.Contains(t, config.Var().String(), "DimensionsOrder")
	assert.Contains(t, config.String(), "<config object>")
}

func TestParseListenFromTimeout(t *testing.T) {
	config, err := decodeConfig([]byte(`{"ListenFrom":[{"Timeout":"3s"}]}`))
	assert.Nil(t, err)
	assert.Equal(t, *config.ListenFrom[0].TimeoutDuration, time.Second*3, "Shouldn't fail parsing")
	config, err = decodeConfig([]byte(`{"ListenFrom":[{"Timeout":"3r"}]}`))
	assert.Error(t, err)
}

func TestFileLoading(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()

	err := ioutil.WriteFile(filename, []byte(`{"ListenFrom":[{"Timeout":"3s"}]}`), os.FileMode(0644))
	assert.Nil(t, err)
	defer func() {
		assert.NoError(t, os.Remove(filename))
	}()
	_, err = loadConfig(filename)
	assert.Nil(t, err)

}

func TestLoad(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer func() {
		assert.NoError(t, os.Remove(filename))
	}()

	err := ioutil.WriteFile(filename, []byte(`{"ListenFrom":[{"Timeout":"3s"}]}`), os.FileMode(0644))

	_, err = Load(filename, log.Discard)
	prev := xdgbasedirGetConfigFileLocation
	xdgbasedirGetConfigFileLocation = func(string) (string, error) { return "", errors.New("bad") }
	defer func() { xdgbasedirGetConfigFileLocation = prev }()
	_, err = Load(filename, log.Discard)
	assert.Equal(t, "bad", fmt.Sprintf("%s", err), "Expect error when xdg loading fails")
	xdgbasedirGetConfigFileLocation = func(string) (string, error) { return filename, nil }
	_, err = Load(filename, log.Discard)
	assert.Nil(t, err)

}

func TestDecodeConfig(t *testing.T) {
	validConfigs := []string{`{}`, `{"host": "192.168.10.2"}`}
	for _, config := range validConfigs {
		_, err := decodeConfig([]byte(config))
		assert.Nil(t, err)
	}
}

func TestConfigTimeout(t *testing.T) {
	configStr := `{"ForwardTo":[{"Type": "thrift", "Timeout": "3s"}]}`
	config, err := decodeConfig([]byte(configStr))
	assert.Nil(t, err)
	if len(config.ForwardTo) != 1 {
		t.Error("Expected length one for forward to")
	}
	if *config.ForwardTo[0].TimeoutDuration != time.Second*3 {
		t.Error("Expected 3 sec timeout")
	}
}

func TestGracefulDurations(t *testing.T) {
	convey.Convey("set up a valid config with durations", t, func() {
		config, err := decodeConfig([]byte(`{"MaxGracefulWaitTime":"1s","GracefulCheckInterval":"1s","SilentGracefulTime":"1s"}`))
		convey.So(err, convey.ShouldBeNil)
		convey.So(*config.GracefulCheckIntervalDuration, convey.ShouldEqual, time.Second)
		convey.So(*config.MaxGracefulWaitTimeDuration, convey.ShouldEqual, time.Second)
		convey.So(*config.SilentGracefulTimeDuration, convey.ShouldEqual, time.Second)
	})
	convey.Convey("set up an invalid config with bad durations", t, func() {
		_, err := decodeConfig([]byte(`{"MaxGracefulWaitTime":"1z"}`))
		convey.So(err, convey.ShouldNotBeNil)
		_, err = decodeConfig([]byte(`{"GracefulCheckInterval":"1z"}`))
		convey.So(err, convey.ShouldNotBeNil)
		_, err = decodeConfig([]byte(`{"SilentGracefulTime":"1z"}`))
		convey.So(err, convey.ShouldNotBeNil)
	})

}
