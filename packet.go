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

// PacketPoolSize is the maximum number of recycled packets a manager keeps (NetConstants.PacketPoolSize).
const PacketPoolSize = 1000

// packetPool mirrors NetPacketPool: a bounded stack of reusable packets, one per manager.
// ponytail: one mutex per manager, faster than sync.Pool on the single-goroutine receive and
// update paths but ~3x slower under heavy parallel Send; shard it if profiles show contention.
type packetPool struct {
	mu    sync.Mutex
	count int
	items [PacketPoolSize]*packet
}

// get mirrors NetPacketPool.GetPacket; packets above maxPacketSize are never taken from the pool.
func (pp *packetPool) get(size int, clearData bool) *packet {
	var p *packet
	if size <= maxPacketSize {
		pp.mu.Lock()
		if pp.count > 0 {
			pp.count--
			p = pp.items[pp.count]
			pp.items[pp.count] = nil
		}
		pp.mu.Unlock()
	}
	if p == nil {
		return &packet{data: make([]byte, size, max(size, maxPacketSize))}
	}
	p.data = p.data[:size]
	if clearData {
		clear(p.data)
	}
	return p
}

func (pp *packetPool) getWithProperty(prop packetProperty, size int) *packet {
	p := pp.get(size+propertyHeaderSize(prop), true)
	p.setProperty(prop)
	return p
}

func (pp *packetPool) getWithData(prop packetProperty, data []byte) *packet {
	p := pp.getWithProperty(prop, len(data))
	copy(p.data[propertyHeaderSize(prop):], data)
	return p
}

// recycle mirrors NetPacketPool.Recycle: big packets are not pooled and a full pool drops the packet.
func (pp *packetPool) recycle(p *packet) {
	if cap(p.data) > maxPacketSize {
		return
	}
	p.data = p.data[:1]
	p.data[0] = 0 // clean fragmented flag
	pp.mu.Lock()
	if pp.count < PacketPoolSize {
		pp.items[pp.count] = p
		pp.count++
	}
	pp.mu.Unlock()
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
