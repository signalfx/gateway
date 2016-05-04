package protocol

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testCloser struct {
	count int
}

func (c *testCloser) Close() {
	c.count++
}

type errCloser struct {
}

var errClose = errors.New("close err")

func (c *errCloser) Close() error {
	return errClose
}

func TestUneventfulForwarder(t *testing.T) {
	u := UneventfulForwarder{nil}
	assert.Equal(t, u.AddEvents(nil, nil), nil)
	assert.Equal(t, int64(0), u.BufferSize())
}

func TestCompositeCloser(t *testing.T) {
	t1 := &testCloser{}
	t2 := &errCloser{}
	assert.Equal(t, errClose, CompositeCloser(t2, OkCloser(t1.Close)).Close())
}

func TestDimMakers(t *testing.T) {
	_, exists := ListenerDims("a", "b")["name"]
	assert.True(t, exists)

	_, exists = ForwarderDims("a", "b")["name"]
	assert.True(t, exists)
}
