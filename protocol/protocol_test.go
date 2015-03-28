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

func TestCompositeCloser(t *testing.T) {
	t1 := &testCloser{}
	t2 := &errCloser{}
	assert.Equal(t, errClose, CompositeCloser(t2, OkCloser(t1.Close)).Close())
}
