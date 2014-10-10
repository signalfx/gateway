package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"
	"github.com/cep21/gohelpers/a"
)

func TestInvalidConfig(t *testing.T) {
	_, err := loadConfig("/tmpasdfads/asdffadssadffads")
	a.ExpectNotEquals(t, err, nil, "Expected non nil error")
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
	a.ExpectNotEquals(t, err, nil, "Expected error parson json")
}

func TestParseStatsDelay(t *testing.T) {
	config, _ := decodeConfig([]byte(`{"StatsDelay":"3s"}`))
	a.ExpectEquals(t, *config.StatsDelayDuration, time.Second*3, "Bad time parsing")
	_, err := decodeConfig([]byte(`{"StatsDelay":"3r"}`))
	a.ExpectNotEquals(t, nil, err, "Shouldn't accept bad time parsing")
}

func TestParseForwardTimeout(t *testing.T) {
	config, err := decodeConfig([]byte(`{"ForwardTo":[{"Timeout":"3s"}]}`))
	a.ExpectEquals(t, nil, err, "Shouldn't fail parsing")
	a.ExpectEquals(t, *config.ForwardTo[0].TimeoutDuration, time.Second*3, "Shouldn't fail parsing")
	config, err = decodeConfig([]byte(`{"ForwardTo":[{"Timeout":"3r"}]}`))
	a.ExpectNotEquals(t, nil, err, "Shouldn't accept bad time parsing")
}

func TestParseListenFromTimeout(t *testing.T) {
	config, err := decodeConfig([]byte(`{"ListenFrom":[{"Timeout":"3s"}]}`))
	a.ExpectEquals(t, nil, err, "Shouldn't fail parsing")
	a.ExpectEquals(t, *config.ListenFrom[0].TimeoutDuration, time.Second*3, "Shouldn't fail parsing")
	config, err = decodeConfig([]byte(`{"ListenFrom":[{"Timeout":"3r"}]}`))
	a.ExpectNotEquals(t, nil, err, "Shouldn't accept bad time parsing")
}

func TestFileLoading(t *testing.T) {
	filename := "/tmp/TestFileLoading.txt"
	err := ioutil.WriteFile(filename, []byte(`{"ListenFrom":[{"Timeout":"3s"}]}`), os.FileMode(0644))
	a.ExpectEquals(t, nil, err, "Shouldn't fail creating file")
	defer os.Remove(filename)
	_, err = loadConfig(filename)
	a.ExpectEquals(t, nil, err, "Creation should not fail")

}

func TestLoadConfig(t *testing.T) {
	filename := "/tmp/doesnotexist.txt"
	_, err := LoadConfig(filename)
	a.ExpectNotEquals(t, nil, err, "File doesn't exist")
	err = ioutil.WriteFile(filename, []byte(`{"ListenFrom":[{"Timeout":"3s"}]}`), os.FileMode(0644))
	defer os.Remove(filename)
	_, err = LoadConfig(filename)
	xdgbasedirGetConfigFileLocation = func(string) (string, error) { return "", errors.New("bad") }
	_, err = LoadConfig(filename)
	a.ExpectEquals(t, "bad", fmt.Sprintf("%s", err), "Expect error when xdg loading fails")
}

func TestDecodeConfig(t *testing.T) {
	validConfigs := []string{`{}`, `{"host": "192.168.10.2"}`}
	for _, config := range validConfigs {
		_, err := decodeConfig([]byte(config))
		if err != nil {
			t.Error("Expected no error loading config!")
		}
	}
}

func TestConfigTimeout(t *testing.T) {
	configStr := `{"ForwardTo":[{"Type": "thrift", "Timeout": "3s"}]}`
	config, err := decodeConfig([]byte(configStr))
	if err != nil {
		t.Error("Expected no error loading config!")
	}
	if len(config.ForwardTo) != 1 {
		t.Error("Expected length one for forward to")
	}
	if *config.ForwardTo[0].TimeoutDuration != time.Second*3 {
		t.Error("Expected 3 sec timeout")
	}
}
