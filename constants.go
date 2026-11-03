// Package litenetlib is a Go port of the LiteNetLib v0.8.3 server.
// It is wire- and behavior-compatible with unmodified v0.8.3 .NET clients.
package litenetlib

// DeliveryMethod selects how a message is sent (NetConstants.cs).
type DeliveryMethod byte

const (
	// Unreliable packets can be dropped, duplicated or arrive without order.
	Unreliable DeliveryMethod = iota
	// ReliableUnordered packets always arrive, but without order.
	ReliableUnordered
	// Sequenced packets can be dropped, but never duplicated and arrive in order.
	Sequenced
	// ReliableOrdered packets always arrive, in order.
	ReliableOrdered
	// ReliableSequenced reliably delivers only the last packet.
	ReliableSequenced
)

// Constants from NetConstants.cs v0.8.3; the exported ones are public there too.
const (
	DefaultWindowSize   = 64
	SocketBufferSize    = 1024 * 1024
	SocketTTL           = 255
	HeaderSize          = 1
	SequencedHeaderSize = 3
	FragmentHeaderSize  = 6
	MaxSequence         = 32768
	HalfMaxSequence     = MaxSequence / 2
	protocolID          = 7
	maxUDPHeaderSize    = 68
	MaxConnectionNumber = 4
)

var possibleMtu = [...]int{
	576 - maxUDPHeaderSize,  // minimal
	1232 - maxUDPHeaderSize, //
	1460 - maxUDPHeaderSize, // google cloud
	1472 - maxUDPHeaderSize, // VPN
	1492 - maxUDPHeaderSize, // Ethernet with LLC and SNAP, PPPoE (RFC 1042)
	1500 - maxUDPHeaderSize, // Ethernet II (RFC 1191)
}

const maxPacketSize = 1500 - maxUDPHeaderSize

// relativeSequenceNumber mirrors NetUtils.RelativeSequenceNumber.
func relativeSequenceNumber(number, expected int) int {
	return (number-expected+MaxSequence+HalfMaxSequence)%MaxSequence - HalfMaxSequence
}
