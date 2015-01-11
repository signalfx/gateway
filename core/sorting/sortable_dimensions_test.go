package sorting

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewOrderedDimensionComparor(t *testing.T) {
	comp := NewOrderedDimensionComparor([]string{"name", "ignored", "value"})
	dims := map[string]string{
		"name":  "jack",
		"a":     "test",
		"value": "big",
		"part2": "goodbye",
		"part1": "hello",
	}
	res := SortDimensions(comp, dims)
	assert.Equal(t, []string{"name", "value", "a", "part1", "part2"}, res)
}
