package stats

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGolangStatKeeper(t *testing.T) {
	s := NewGolangStatKeeper()
	assert.Equal(t, 29, len(s.GetStats()), "Expected 29 stats")
}
