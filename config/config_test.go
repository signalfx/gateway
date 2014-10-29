package config

import (
	"errors"
	"fmt"
	"github.com/cep21/gohelpers/a"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestInvalidConfig(t *testing.T) {
	_, err := LoadConfig("invalidodesnotexist___SDFSDFSD")
	a.ExpectNotNil(t, err)
}

func TestStringConv(t *testing.T) {
	name := string("aname")
	lf := ListenFrom{Name: &name}
	a.ExpectContains(t, lf.String(), "aname", "Unable to find expected string")

	ft := ForwardTo{Name: &name}
	a.ExpectContains(t, ft.String(), "aname", "Unable to find expected string")
}

func TestBadDecode(t *testing.T) {
	_, err := decodeConfig([]byte("badjson"))
	a.ExpectNotNil(t, err)
}

func TestParseStatsDelay(t *testing.T) {
	config, _ := decodeConfig([]byte(`{"StatsDelay":"3s"}`))
	a.ExpectEquals(t, *config.StatsDelayDuration, time.Second*3, "Bad time parsing")
	_, err := decodeConfig([]byte(`{"StatsDelay":"3r"}`))
	a.ExpectNotNil(t, err)
}

func TestParseForwardTimeout(t *testing.T) {
	config, err := decodeConfig([]byte(`{"ForwardTo":[{"Timeout":"3s"}]}`))
	a.ExpectNil(t, err)
	a.ExpectEquals(t, *config.ForwardTo[0].TimeoutDuration, time.Second*3, "Shouldn't fail parsing")
	config, err = decodeConfig([]byte(`{"ForwardTo":[{"Timeout":"3r"}]}`))
	a.ExpectNotNil(t, err)
}

func TestParseListenFromTimeout(t *testing.T) {
	config, err := decodeConfig([]byte(`{"ListenFrom":[{"Timeout":"3s"}]}`))
	a.ExpectNil(t, err)
	a.ExpectEquals(t, *config.ListenFrom[0].TimeoutDuration, time.Second*3, "Shouldn't fail parsing")
	config, err = decodeConfig([]byte(`{"ListenFrom":[{"Timeout":"3r"}]}`))
	a.ExpectNotNil(t, err)
}

func TestFileLoading(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer os.Remove(filename)

	err := ioutil.WriteFile(filename, []byte(`{"ListenFrom":[{"Timeout":"3s"}]}`), os.FileMode(0644))
	a.ExpectNil(t, err)
	defer os.Remove(filename)
	_, err = loadConfig(filename)
	a.ExpectNil(t, err)

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
	a.ExpectEquals(t, "bad", fmt.Sprintf("%s", err), "Expect error when xdg loading fails")
	xdgbasedirGetConfigFileLocation = func(string) (string, error) { return filename, nil }
	_, err = LoadConfig(filename)
	a.ExpectNil(t, err)

}

func TestDecodeConfig(t *testing.T) {
	validConfigs := []string{`{}`, `{"host": "192.168.10.2"}`}
	for _, config := range validConfigs {
		_, err := decodeConfig([]byte(config))
		a.ExpectNil(t, err)
	}
}

func TestConfigTimeout(t *testing.T) {
	configStr := `{"ForwardTo":[{"Type": "thrift", "Timeout": "3s"}]}`
	config, err := decodeConfig([]byte(configStr))
	a.ExpectNil(t, err)
	if len(config.ForwardTo) != 1 {
		t.Error("Expected length one for forward to")
	}
	if *config.ForwardTo[0].TimeoutDuration != time.Second*3 {
		t.Error("Expected 3 sec timeout")
	}
}
