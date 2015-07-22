package event

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEvent(t *testing.T) {
	dp := New("eventType", "category", map[string]string{}, time.Now())
	assert.Contains(t, dp.String(), "eventType")
	assert.Contains(t, dp.String(), "category")
}
