package dpdimsort

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewOrdering(t *testing.T) {
	comp := NewOrdering([]string{"name", "ignored", "value"})
	dims := map[string]string{
		"name":  "jack",
		"a":     "test",
		"value": "big",
		"part2": "goodbye",
		"part1": "hello",
	}
	res := comp.Sort(dims)
	assert.Equal(t, []string{"name", "value", "a", "part1", "part2"}, res)
}

func TestLessOrderer(t *testing.T) {
	v := &orderedOrdering{
		dimensionOrderMap: map[string]int{
			"b": 0,
		},
	}
	currentOrder := []string{"a", "b"}
	m := map[string]string{}
	assert.False(t, v.Less(m, currentOrder, 0, 1))
}
