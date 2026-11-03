//go:build unix

package litenetlib

import (
	"runtime"
	"syscall"
)

// configureSocket applies the NetSocket.BindSocket options.
func configureSocket(network string, fd uintptr, reuseAddress bool) {
	s := int(fd)
	if reuseAddress {
		_ = syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	}
	if network == "udp4" {
		setTTL(fd, SocketTTL)
		// DontFragment; the reference skips it on macOS (NETCORE build).
		if runtime.GOOS == "linux" {
			const ipMtuDiscover, ipPmtudiscDo = 10, 2
			if err := syscall.SetsockoptInt(s, syscall.IPPROTO_IP, ipMtuDiscover, ipPmtudiscDo); err != nil {
				writeError("[B]DontFragment error: %v", err)
			}
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
	_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_TTL, ttl)
}

// Socket errors that NetSocket and NetManager handle specially.
var (
	errMessageSize     error = syscall.EMSGSIZE
	errHostUnreachable error = syscall.EHOSTUNREACH
	errConnectionReset error = syscall.ECONNRESET
	errTimedOut        error = syscall.ETIMEDOUT
	errNoBufferSpace   error = syscall.ENOBUFS
)
