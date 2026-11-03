package litenetlib

import (
	"context"
	"net"
	"net/netip"
	"syscall"
)

func listenUDP(network string, addr netip.AddrPort, reuseAddress bool) (*net.UDPConn, error) {
	lc := net.ListenConfig{Control: func(network, _ string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) { configureSocket(network, fd, reuseAddress) })
	}}
	pc, err := lc.ListenPacket(context.Background(), network, addr.String())
	if err != nil {
		return nil, err
	}
	conn := pc.(*net.UDPConn)
	_ = conn.SetReadBuffer(SocketBufferSize)
	_ = conn.SetWriteBuffer(SocketBufferSize)
	return conn, nil
}

// setTTL sets the IPv4 socket TTL (NetSocket.Ttl).
func (m *Manager) setTTL(ttl int) {
	if rc, err := m.conn4.SyscallConn(); err == nil {
		_ = rc.Control(func(fd uintptr) { setTTL(fd, ttl) })
	}
}

// writeTo sends directly on the socket, bypassing statistics (NetSocket.SendTo).
func (m *Manager) writeTo(data []byte, addr netip.AddrPort) {
	conn := m.conn4
	if m.conn6 != nil && addr.Addr().Is6() && !addr.Addr().Is4In6() {
		conn = m.conn6
	}
	if conn != nil {
		_, _ = conn.WriteToUDPAddrPort(data, addr)
	}
}
