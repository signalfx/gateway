package nettest

import "net"

// A NetworkListener is a listener that looks for data on a network address.  It is sometimes
// useful in testing to get this address so you can talk to it directly.
type NetworkListener interface {
	Addr() net.Addr
}

// TcpPort of the listener address.  If the listener isn't TCP, this may panic()
func TcpPort(l NetworkListener) uint16 {
	return (uint16)(l.Addr().(*net.TCPAddr).Port)
}
