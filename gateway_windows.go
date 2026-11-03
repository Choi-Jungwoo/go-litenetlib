package litenetlib

import (
	"errors"
	"net"
	"unsafe"

	"golang.org/x/sys/windows"
)

// gatewayInterfaces reports which adapters list a gateway, like .NET's
// SystemIPInterfaceProperties.GatewayAddresses (GetAdaptersAddresses with gateways).
func gatewayInterfaces() func(net.Interface) bool {
	indexes := map[int]bool{}
	size := uint32(15000)
	for range 3 {
		buf := make([]byte, size)
		aa := (*windows.IpAdapterAddresses)(unsafe.Pointer(&buf[0]))
		err := windows.GetAdaptersAddresses(windows.AF_UNSPEC, windows.GAA_FLAG_INCLUDE_GATEWAYS, 0, aa, &size)
		if errors.Is(err, windows.ERROR_BUFFER_OVERFLOW) {
			continue
		}
		if err != nil {
			break
		}
		for ; aa != nil; aa = aa.Next {
			if aa.FirstGatewayAddress == nil {
				continue
			}
			index := aa.IfIndex // net.Interface.Index, as Go's interfaceTable computes it
			if index == 0 {
				index = aa.Ipv6IfIndex
			}
			indexes[int(index)] = true
		}
		break
	}
	return func(ifi net.Interface) bool { return indexes[ifi.Index] }
}
