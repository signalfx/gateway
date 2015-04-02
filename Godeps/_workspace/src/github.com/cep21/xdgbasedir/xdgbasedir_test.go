package xdgbasedir

import (
	"os"
	"os/user"
	"testing"
)

func resetEnv() {
	// Reset overridden functions that were changed during testing
	osGetEnv = os.Getenv
	userCurrent = user.Current
	osStat = os.Stat
	osIsNotExist = os.IsNotExist
}

func expectEquals(t *testing.T, expected string, given string, msg string) {
	if expected != given {
		t.Error("Expected", expected, "not equal to given", given, ":", msg)
	}
}

func TestGetAll(t *testing.T) {
	userCurrent = func() (*user.User, error) {
		return &user.User{HomeDir: "/home/Person"}, nil
	}
	defer resetEnv()

	data_home, err := DataHomeDirectory()
	if err != nil {
		t.Error("Unexpected error ", err)
	}
	expectEquals(t, "/home/Person/.local/share", data_home, "Unexpected data")

	config_home, err := ConfigHomeDirectory()
	if err != nil {
		t.Error("Unexpected error ", err)
	}
	expectEquals(t, "/home/Person/.config", config_home, "Unexpected config")

	cache_home, err := CacheDirectory()
	if err != nil {
		t.Error("Unexpected error ", err)
	}
	expectEquals(t, "/home/Person/.cache", cache_home, "Unexpected cache")
}

func TestDirectories(t *testing.T) {
	userCurrent = func() (*user.User, error) {
		return &user.User{HomeDir: "/home/Person"}, nil
	}
	defer resetEnv()

	location, err := GetDataFileLocation("name")
	if err != nil {
		t.Error("Unexpected error ", err)
	}
	expectEquals(t, "/home/Person/.local/share/name", location, "Data file location")

	osGetEnv = func(string) string { return "/var/location/default" }

	location, err = GetDataFileLocation("name")
	if err != nil {
		t.Error("Unexpected error ", err)
	}
	expectEquals(t, "/var/location/default/name", location, "Data file location")
}
