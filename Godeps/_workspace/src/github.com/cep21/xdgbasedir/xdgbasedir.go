// Pulled from http://standards.freedesktop.org/basedir-spec/basedir-spec-latest.html
//
// Go helper functions for data/config/cache directory and file lookup
package xdgbasedir

import (
	"os"
	"os/user"
	"path"
	"strings"
)

const (
	defaultDataHomeDirectory = "/.local/share"
	defaultConfigDirectory   = "/.config"
	defaultCacheDirectory    = "/.cache"
)

var (
	// Overridable for testing
	osGetEnv     = os.Getenv
	userCurrent  = user.Current
	osStat       = os.Stat
	osIsNotExist = os.IsNotExist
)

//  Single base directory relative to which user-specific data files should be written
func DataHomeDirectory() (string, error) {
	return getInEnvOrJoinWithHome("XDG_DATA_HOME", defaultDataHomeDirectory)
}

// Get the location of a data file
func GetDataFileLocation(filename string) (string, error) {
	return fileLocationRetrievalHelper(filename, dataDirectories(), DataHomeDirectory)
}

func dataDirectories() []string {
	return uniqueDirsOnVariable("XDG_DATA_DIRS", "/usr/local/share/:/usr/share/")
}

//  Single base directory relative to which user-specific config files should be written
func ConfigHomeDirectory() (string, error) {
	return getInEnvOrJoinWithHome("XDG_CONFIG_HOME", defaultConfigDirectory)
}

// Get the location of a config file
func GetConfigFileLocation(filename string) (string, error) {
	return fileLocationRetrievalHelper(filename, configDirectories(), ConfigHomeDirectory)
}

func configDirectories() []string {
	return uniqueDirsOnVariable("XDG_CONFIG_DIRS", "/etc/xdg")
}

//  Single base directory relative to which user specific non-essential data files should be stored.
func CacheDirectory() (string, error) {
	return getInEnvOrJoinWithHome("XDG_CACHE_HOME", defaultCacheDirectory)
}

func GetCacheFileLocation(filename string) (string, error) {
	return fileLocationRetrievalHelper(filename, []string{}, CacheDirectory)
}

func fileLocationRetrievalHelper(filename string, dirs []string, defaultDirectoryFunc func() (string, error)) (string, error) {
	// The default location should be checked first
	default_dir, defaultDirectoryError := defaultDirectoryFunc()
	if defaultDirectoryError != nil {
		dirs = append([]string{default_dir}, dirs...)
	}

	for _, dir := range dirs {
		file_loc := path.Join(dir, filename)
		fileInfo, err := osStat(file_loc)
		if osIsNotExist(err) {
			continue
		}

		return fileInfo.Name(), nil
	}
	return path.Join(default_dir, filename), defaultDirectoryError
}

func uniqueDirsOnVariable(envVar string, defaultVal string) []string {
	data_dirs := osGetEnv(envVar)
	if data_dirs == "" {
		data_dirs = defaultVal
	}
	return splitAndReturnUnique(data_dirs, ":")
}

func splitAndReturnUnique(str string, sep string) []string {
	parts := strings.Split(str, sep)
	var ret []string
	usedMap := make(map[string]bool)
	for _, p := range parts {
		_, found := usedMap[p]
		if !found {
			usedMap[p] = true
			ret = append(ret, p)
		}
	}
	return ret
}

func getInEnvOrJoinWithHome(env_name string, directory string) (string, error) {
	config_home := osGetEnv(env_name)
	if config_home != "" {
		return config_home, nil
	}
	return joinWithHome(directory)
}

func joinWithHome(dir string) (string, error) {
	usr, err := userCurrent()
	if err != nil {
		return "", err
	}
	return path.Join(usr.HomeDir, dir), nil
}
