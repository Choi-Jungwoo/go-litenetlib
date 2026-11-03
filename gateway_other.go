//go:build unix && !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd

package litenetlib

import "net"

// gatewayInterfaces cannot read the routing table here (.NET does not run on these
// systems either), so every interface counts as having a gateway.
func gatewayInterfaces() func(net.Interface) bool {
	return func(net.Interface) bool { return true }
}
