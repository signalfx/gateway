package dptest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicStreamer(t *testing.T) {
	b := BasicStreamer{
		Chan: nil,
	}
	assert.Nil(t, b.Channel())
	assert.Equal(t, "", b.Name())
}
