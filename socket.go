package litenetlib

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"syscall"
	"time"
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

// sendTimeout is the reference's Socket.SendTimeout: a send that cannot complete fails
// like any other socket error instead of stalling the caller, who may hold a peer lock.
const sendTimeout = 500 * time.Millisecond

// sockets is the socket pair of one Start..Stop session, published as a unit so a send
// never mixes sessions.
type sockets struct {
	v4, v6   *net.UDPConn
	mu4, mu6 sync.Mutex
}

func (s *sockets) writeTo(data []byte, addr netip.AddrPort) (int, error) {
	conn, mu := s.v4, &s.mu4
	if s.v6 != nil && addr.Addr().Is6() && !addr.Addr().Is4In6() {
		conn, mu = s.v6, &s.mu6
	}
	// SetWriteDeadline also moves the deadline of a write already blocked, so sends take
	// turns to each get their own timeout. Go's fd write lock serializes them anyway.
	mu.Lock()
	defer mu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(sendTimeout))
	return conn.WriteToUDPAddrPort(data, addr)
}

// setTTL sets the IPv4 socket TTL (NetSocket.Ttl).
func (m *Manager) setTTL(ttl int) {
	if s := m.socks.Load(); s != nil {
		if rc, err := s.v4.SyscallConn(); err == nil {
			_ = rc.Control(func(fd uintptr) { setTTL(fd, ttl) })
		}
	}
}

// writeTo sends directly on the socket, bypassing statistics (NetSocket.SendTo).
func (m *Manager) writeTo(data []byte, addr netip.AddrPort) {
	if s := m.socks.Load(); s != nil {
		_, _ = s.writeTo(data, addr)
	}
}
