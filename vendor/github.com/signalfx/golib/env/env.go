package env

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// GetStringEnvVar returns the given env var key's value or the default value
func GetStringEnvVar(envVar string, def *string) *string {
	if val := os.Getenv(envVar); val != "" {
		return &val
	}

	// return a copy of the default
	if def != nil {
		ret := *def
		return &ret
	}

	return nil
}

// GetDurationEnvVar returns the given env var key's value as a Duration or the default value
func GetDurationEnvVar(envVar string, def *time.Duration) *time.Duration {
	if strVal := os.Getenv(envVar); strVal != "" {
		if dur, err := time.ParseDuration(strVal); err == nil {
			return &dur
		}
	}

	// return a copy of the default
	if def != nil {
		ret := *def
		return &ret
	}

	return nil
}

// GetUintEnvVar returns the given env var key's value as a uint or the default value
func GetUintEnvVar(envVar string, def *uint) *uint {
	if strVal := os.Getenv(envVar); strVal != "" {
		if parsedVal, err := strconv.ParseUint(strVal, 10, 16); err == nil {
			val := uint(parsedVal)
			return &val
		}
	}

	// return a copy of the default
	if def != nil {
		ret := *def
		return &ret
	}

	return nil
}

// GetUint64EnvVar returns the given env var key's value as a uint64 or the default value
func GetUint64EnvVar(envVar string, def *uint64) *uint64 {
	if strVal := os.Getenv(envVar); strVal != "" {
		if parsedVal, err := strconv.ParseUint(strVal, 10, 64); err == nil {
			return &parsedVal
		}
	}

	// return a copy of the default
	if def != nil {
		ret := *def
		return &ret
	}

	return nil
}

// GetCommaSeparatedStringEnvVar returns the given env var key's value split by comma or the default values
func GetCommaSeparatedStringEnvVar(envVar string, def []string) []string {
	if val := os.Getenv(envVar); val != "" {
		return strings.Split(strings.Replace(val, " ", "", -1), ",")
	}

	// return a copy of the default
	var ret = make([]string, len(def))
	for i, s := range def {
		ret[i] = s
	}

	return ret
}
