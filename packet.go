package litenetlib

import (
	"encoding/binary"
	"sync"
)

// packetProperty is the 5-bit packet type in the first header byte (NetPacket.cs).
type packetProperty byte

const (
	propUnreliable packetProperty = iota
	propReliableUnordered
	propSequenced
	propReliableOrdered
	propAckReliable
	propAckReliableOrdered
	propPing
	propPong
	propConnectRequest
	propConnectAccept
	propDisconnect
	propUnconnectedMessage
	propNatIntroductionRequest
	propNatIntroduction
	propNatPunchMessage
	propMtuCheck
	propMtuOk
	propDiscoveryRequest
	propDiscoveryResponse
	propMerged
	propShutdownOk
	propReliableSequenced
	propAckReliableSequenced
	propPeerNotFound
	propInvalidProtocol
	lastProperty = propInvalidProtocol
)

const (
	connectRequestHeaderSize = 13
	connectAcceptSize        = 11
)

// packet is a NetPacket: len(data) is the packet size, cap(data) the buffer.
type packet struct {
	data []byte
}

func (p *packet) property() packetProperty { return packetProperty(p.data[0] & 0x1F) }
func (p *packet) setProperty(v packetProperty) {
	p.data[0] = p.data[0]&0xE0 | byte(v)
}
func (p *packet) connectionNumber() byte { return (p.data[0] & 0x60) >> 5 }
func (p *packet) setConnectionNumber(v byte) {
	p.data[0] = p.data[0]&0x9F | v<<5
}
func (p *packet) sequence() uint16       { return binary.LittleEndian.Uint16(p.data[1:]) }
func (p *packet) setSequence(v uint16)   { binary.LittleEndian.PutUint16(p.data[1:], v) }
func (p *packet) isFragmented() bool     { return p.data[0]&0x80 != 0 }
func (p *packet) markFragmented()        { p.data[0] |= 0x80 }
func (p *packet) fragmentID() uint16     { return binary.LittleEndian.Uint16(p.data[3:]) }
func (p *packet) fragmentPart() uint16   { return binary.LittleEndian.Uint16(p.data[5:]) }
func (p *packet) fragmentsTotal() uint16 { return binary.LittleEndian.Uint16(p.data[7:]) }
func (p *packet) headerSize() int        { return propertyHeaderSize(p.property()) }
func (p *packet) setFragment(id, part, total uint16) {
	binary.LittleEndian.PutUint16(p.data[3:], id)
	binary.LittleEndian.PutUint16(p.data[5:], part)
	binary.LittleEndian.PutUint16(p.data[7:], total)
}

func propertyHeaderSize(prop packetProperty) int {
	switch prop {
	case propReliableOrdered, propReliableUnordered, propReliableSequenced, propSequenced,
		propAckReliable, propAckReliableOrdered, propAckReliableSequenced, propPing:
		return SequencedHeaderSize
	case propConnectRequest:
		return connectRequestHeaderSize
	case propConnectAccept:
		return connectAcceptSize
	case propDisconnect:
		return HeaderSize + 8
	case propPong:
		return SequencedHeaderSize + 8
	default:
		return HeaderSize
	}
}

// newPacket allocates an unpooled packet (used for long-lived control packets).
func newPacket(prop packetProperty, size int) *packet {
	p := &packet{data: make([]byte, size+propertyHeaderSize(prop))}
	p.setProperty(prop)
	return p
}

var packetPool = sync.Pool{New: func() any { return &packet{data: make([]byte, 0, maxPacketSize)} }}

// getPacket mirrors NetPacketPool.GetPacket; oversized packets are not pooled.
func getPacket(size int, clearData bool) *packet {
	if size > maxPacketSize {
		return &packet{data: make([]byte, size)}
	}
	p := packetPool.Get().(*packet)
	p.data = p.data[:size]
	if clearData {
		clear(p.data)
	}
	return p
}

func getWithProperty(prop packetProperty, size int) *packet {
	p := getPacket(size+propertyHeaderSize(prop), true)
	p.setProperty(prop)
	return p
}

func getWithData(prop packetProperty, data []byte) *packet {
	p := getWithProperty(prop, len(data))
	copy(p.data[propertyHeaderSize(prop):], data)
	return p
}

func recyclePacket(p *packet) {
	if cap(p.data) != maxPacketSize {
		return // don't pool big packets
	}
	p.data = p.data[:1]
	p.data[0] = 0 // clean fragmented flag
	packetPool.Put(p)
}

// fromBytes mirrors NetPacket.FromBytes validation.
func (p *packet) fromBytes(src []byte) bool {
	if len(src) == 0 {
		return false
	}
	prop := packetProperty(src[0] & 0x1F)
	fragmented := src[0]&0x80 != 0
	hs := propertyHeaderSize(prop)
	if prop > lastProperty || len(src) < hs || (fragmented && len(src) < hs+FragmentHeaderSize) {
		return false
	}
	p.data = p.data[:len(src)]
	copy(p.data, src)
	return true
}
