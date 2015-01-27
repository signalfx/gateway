package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGolangStatKeeper(t *testing.T) {
	s := NewGolangStatKeeper()
	assert.Equal(t, 30, len(s.GetStats()))
}
