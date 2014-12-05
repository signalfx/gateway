package config

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestInvalidConfig(t *testing.T) {
	_, err := LoadConfig("invalidodesnotexist___SDFSDFSD")
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
	defer os.Remove(filename)

	err := ioutil.WriteFile(filename, []byte(`{"ListenFrom":[{"Timeout":"3s"}]}`), os.FileMode(0644))
	assert.Nil(t, err)
	defer os.Remove(filename)
	_, err = loadConfig(filename)
	assert.Nil(t, err)

}

func TestLoadConfig(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	err := ioutil.WriteFile(filename, []byte(`{"ListenFrom":[{"Timeout":"3s"}]}`), os.FileMode(0644))
	defer os.Remove(filename)
	_, err = LoadConfig(filename)
	prev := xdgbasedirGetConfigFileLocation
	xdgbasedirGetConfigFileLocation = func(string) (string, error) { return "", errors.New("bad") }
	defer func() { xdgbasedirGetConfigFileLocation = prev }()
	_, err = LoadConfig(filename)
	assert.Equal(t, "bad", fmt.Sprintf("%s", err), "Expect error when xdg loading fails")
	xdgbasedirGetConfigFileLocation = func(string) (string, error) { return filename, nil }
	_, err = LoadConfig(filename)
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
