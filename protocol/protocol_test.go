package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUneventfulForwarder(t *testing.T) {
	u := UneventfulForwarder{nil}
	assert.Equal(t, u.AddEvents(nil, nil), nil)
	assert.Equal(t, u.AddSpans(nil, nil), nil)
	assert.Equal(t, int64(0), u.Pipeline())
}

func TestDimMakers(t *testing.T) {
	_, exists := ListenerDims("a", "b")["name"]
	assert.True(t, exists)

	_, exists = ForwarderDims("a", "b")["name"]
	assert.True(t, exists)
}
