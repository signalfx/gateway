package nettest

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListenerPort(t *testing.T) {
	psocket, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer psocket.Close()
	p := TCPPort(psocket)
	assert.True(t, p > 0)
}
