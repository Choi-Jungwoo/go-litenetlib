//go:build windows

package litenetlib

import "syscall"

// configureSocket applies the NetSocket.BindSocket options.
func configureSocket(network string, fd uintptr, reuseAddress bool) {
	s := syscall.Handle(fd)
	// ExclusiveAddressUse = !reuseAddress, then ReuseAddress = reuseAddress.
	const soExclusiveAddrUse = ^syscall.SO_REUSEADDR // SO_EXCLUSIVEADDRUSE
	exclusive, reuse := 1, 0
	if reuseAddress {
		exclusive, reuse = 0, 1
	}
	err := syscall.SetsockoptInt(s, syscall.SOL_SOCKET, soExclusiveAddrUse, exclusive)
	if err == nil {
		err = syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, reuse)
	}
	if err != nil {
		writeError("IL2CPP SetSocketOption error")
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
		return
	}
	// Receive IPv6 discovery requests sent to the all-nodes multicast group FF02::1.
	mreq := &syscall.IPv6Mreq{Multiaddr: [16]byte{0: 0xFF, 1: 0x02, 15: 0x01}}
	_ = syscall.SetsockoptIPv6Mreq(s, syscall.IPPROTO_IPV6, syscall.IPV6_JOIN_GROUP, mreq)
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
