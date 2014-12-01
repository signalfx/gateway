package stats

import (
	"github.com/cep21/gohelpers/a"
	"testing"
)

func TestGolangStatKeeper(t *testing.T) {
	s := NewGolangStatKeeper()
	a.ExpectEquals(t, 29, len(s.GetStats()), "Expected 29 stats")
}
