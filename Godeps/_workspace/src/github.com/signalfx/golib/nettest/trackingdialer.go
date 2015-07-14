package nettest

import (
	"net"
	"time"
)

// TrackingDialer remembers connections it's made and allows users to close them
type TrackingDialer struct {
	Dialer net.Dialer
	Conns  []net.Conn
}

// Close all stored connections.  Returns an error on the first close that fails
func (t *TrackingDialer) Close() error {
	var retErr error
	for len(t.Conns) != 0 {
		c := t.Conns[0]
		err := c.Close()
		if err != nil {
			retErr = err
		}
		t.Conns = t.Conns[1:]
	}
	return retErr
}

// DialTimeout simulates net.DialTimeout
func (t *TrackingDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	duse := t.Dialer
	duse.Timeout = timeout
	conn, err := duse.Dial(network, address)
	if err != nil {
		return nil, err
	}
	t.Conns = append(t.Conns, conn)
	return conn, err
}
