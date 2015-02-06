package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGolangKeeper(t *testing.T) {
	s := NewGolangKeeper()
	assert.Equal(t, 30, len(s.Stats()))
}
