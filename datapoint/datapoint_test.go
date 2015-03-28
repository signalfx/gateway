package datapoint

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDatapoint(t *testing.T) {
	dp := New("aname", map[string]string{}, nil, Gauge, time.Now())
	assert.Contains(t, dp.String(), "aname")
}
