/**
 * Date: 2/6/14
 * Time: 1:32 PM
 * @author jack
 */
package config

import (
	"testing"
	"time"
)

func TestInvalidConfig(t *testing.T) {
	_, err := loadConfig("/tmpasdfads/asdffadssadffads")
	if err == nil {
		t.Error("Expected error loading config!")
	}
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
