package stats

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGolangStatKeeper(t *testing.T) {
	s := NewGolangStatKeeper()
	assert.Equal(t, 30, len(s.GetStats()))
}
