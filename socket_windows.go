//go:build windows

package litenetlib

import "syscall"

// configureSocket applies the NetSocket.BindSocket options.
func configureSocket(network string, fd uintptr, reuseAddress bool) {
	s := syscall.Handle(fd)
	if reuseAddress {
		_ = syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	}
	if network == "udp4" {
		const ipDontFragment = 14
		setTTL(fd, SocketTTL)
		if err := syscall.SetsockoptInt(s, syscall.IPPROTO_IP, ipDontFragment, 1); err != nil {
			writeError("[B]DontFragment error: %v", err)
		}
		if err := syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1); err != nil {
			writeError("[B]Broadcast error: %v", err)
		}
	}
}

func setTTL(fd uintptr, ttl int) {
	_ = syscall.SetsockoptInt(syscall.Handle(fd), syscall.IPPROTO_IP, syscall.IP_TTL, ttl)
}

// Socket errors that NetSocket and NetManager handle specially (Winsock codes).
var (
	errMessageSize     error = syscall.Errno(10040) // WSAEMSGSIZE
	errHostUnreachable error = syscall.Errno(10065) // WSAEHOSTUNREACH
	errConnectionReset error = syscall.Errno(10054) // WSAECONNRESET
	errTimedOut        error = syscall.Errno(10060) // WSAETIMEDOUT
	errNoBufferSpace   error = syscall.Errno(10055) // WSAENOBUFS
)
