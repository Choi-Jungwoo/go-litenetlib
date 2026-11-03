package litenetlib

import "time"

// All channel methods except enqueue run with peer.mu held.
// Outgoing queues are guarded by peer.queueMu so Send never waits on protocol work.

const bitsInByte = 8

type pendingPacket struct {
	packet    *packet
	timeStamp time.Time
	sent      bool
}

// reliableChannel mirrors ReliableChannel.cs (ordered or unordered).
type reliableChannel struct {
	peer         *Peer
	queue        []*packet
	outgoingAcks *packet
	pending      [DefaultWindowSize]pendingPacket
	received     [DefaultWindowSize]*packet // ordered
	early        [DefaultWindowSize]bool    // unordered

	localSequence     int
	remoteSequence    int
	localWindowStart  int
	remoteWindowStart int
	mustSendAcks      bool
	ordered           bool
}

func (c *reliableChannel) init(peer *Peer, ordered bool) {
	c.peer = peer
	c.ordered = ordered
	prop := propAckReliable
	if ordered {
		prop = propAckReliableOrdered
	}
	c.outgoingAcks = newPacket(prop, (DefaultWindowSize-1)/bitsInByte+1)
}

func (c *reliableChannel) processAck(p *packet) {
	if len(p.data) != len(c.outgoingAcks.data) {
		return // invalid acks packet size
	}
	ackWindowStart := int(p.sequence())
	windowRel := relativeSequenceNumber(c.localWindowStart, ackWindowStart)
	if ackWindowStart >= MaxSequence || windowRel < 0 || windowRel >= DefaultWindowSize {
		return // bad window start or old acks
	}
	acks := p.data
	for seq := c.localWindowStart; seq != c.localSequence; seq = (seq + 1) % MaxSequence {
		if relativeSequenceNumber(seq, ackWindowStart) >= DefaultWindowSize {
			break
		}
		idx := seq % DefaultWindowSize
		if acks[SequencedHeaderSize+idx/bitsInByte]&(1<<(idx%bitsInByte)) == 0 {
			c.peer.stats.PacketLoss++ // false ack
			continue
		}
		if seq == c.localWindowStart {
			c.localWindowStart = (c.localWindowStart + 1) % MaxSequence
		}
		if pp := &c.pending[idx]; pp.packet != nil {
			recyclePacket(pp.packet)
			pp.packet = nil
		}
	}
}

func (c *reliableChannel) sendNextPackets() {
	if c.mustSendAcks {
		c.mustSendAcks = false
		c.peer.sendUserData(c.outgoingAcks)
	}

	now := time.Now()
	c.peer.queueMu.Lock()
	n := 0
	for n < len(c.queue) && relativeSequenceNumber(c.localSequence, c.localWindowStart) < DefaultWindowSize {
		pp := &c.pending[c.localSequence%DefaultWindowSize]
		pp.sent = false
		pp.packet = c.queue[n]
		pp.packet.setSequence(uint16(c.localSequence))
		c.localSequence = (c.localSequence + 1) % MaxSequence
		n++
	}
	c.queue = dequeue(c.queue, n)
	c.peer.queueMu.Unlock()

	resendDelay := time.Duration(c.peer.resendDelay * float64(time.Millisecond))
	for seq := c.localWindowStart; seq != c.localSequence; seq = (seq + 1) % MaxSequence {
		pp := &c.pending[seq%DefaultWindowSize]
		if pp.packet == nil {
			continue
		}
		if pp.sent && now.Sub(pp.timeStamp) < resendDelay {
			continue
		}
		pp.timeStamp = now
		pp.sent = true
		c.peer.sendUserData(pp.packet)
	}
}

func (c *reliableChannel) processPacket(p *packet) {
	seq := int(p.sequence())
	if seq >= MaxSequence {
		return
	}
	relate := relativeSequenceNumber(seq, c.remoteWindowStart)
	if relativeSequenceNumber(seq, c.remoteSequence) > DefaultWindowSize {
		return // bad sequence
	}
	if relate < 0 || relate >= DefaultWindowSize*2 {
		return // too old or too new
	}

	acks := c.outgoingAcks.data
	if relate >= DefaultWindowSize {
		// move window, clearing old acks
		newWindowStart := (c.remoteWindowStart + relate - DefaultWindowSize + 1) % MaxSequence
		c.outgoingAcks.setSequence(uint16(newWindowStart))
		for c.remoteWindowStart != newWindowStart {
			idx := c.remoteWindowStart % DefaultWindowSize
			acks[SequencedHeaderSize+idx/bitsInByte] &^= 1 << (idx % bitsInByte)
			c.remoteWindowStart = (c.remoteWindowStart + 1) % MaxSequence
		}
	}

	c.mustSendAcks = true
	ackIdx := seq % DefaultWindowSize
	ackByte := SequencedHeaderSize + ackIdx/bitsInByte
	ackBit := byte(1) << (ackIdx % bitsInByte)
	if acks[ackByte]&ackBit != 0 {
		return // duplicate
	}
	acks[ackByte] |= ackBit

	if seq == c.remoteSequence {
		c.peer.addIncomingPacket(p)
		c.remoteSequence = (c.remoteSequence + 1) % MaxSequence
		if c.ordered {
			for {
				i := c.remoteSequence % DefaultWindowSize
				held := c.received[i]
				if held == nil {
					break
				}
				c.received[i] = nil
				c.peer.addIncomingPacket(held)
				c.remoteSequence = (c.remoteSequence + 1) % MaxSequence
			}
		} else {
			for c.early[c.remoteSequence%DefaultWindowSize] {
				c.early[c.remoteSequence%DefaultWindowSize] = false
				c.remoteSequence = (c.remoteSequence + 1) % MaxSequence
			}
		}
		return
	}

	if c.ordered {
		c.received[ackIdx] = p
	} else {
		c.early[ackIdx] = true
		c.peer.addIncomingPacket(p)
	}
}

// sequencedChannel mirrors SequencedChannel.cs (unreliable or reliable sequenced).
type sequencedChannel struct {
	peer           *Peer
	queue          []*packet
	localSequence  int
	remoteSequence uint16
	reliable       bool
	lastPacket     *packet
	ackPacket      *packet
	mustSendAck    bool
}

func (c *sequencedChannel) init(peer *Peer, reliable bool) {
	c.peer = peer
	c.reliable = reliable
	if reliable {
		c.ackPacket = newPacket(propAckReliableSequenced, 0)
	}
}

func (c *sequencedChannel) sendNextPackets() {
	c.peer.queueMu.Lock()
	if c.reliable && len(c.queue) == 0 {
		if c.lastPacket != nil {
			c.peer.sendUserData(c.lastPacket)
		}
	} else {
		for i, p := range c.queue {
			c.localSequence = (c.localSequence + 1) % MaxSequence
			p.setSequence(uint16(c.localSequence))
			c.peer.sendUserData(p)
			if c.reliable && i == len(c.queue)-1 {
				c.lastPacket = p
			} else {
				recyclePacket(p)
			}
		}
		c.queue = dequeue(c.queue, len(c.queue))
	}
	c.peer.queueMu.Unlock()

	if c.reliable && c.mustSendAck {
		c.mustSendAck = false
		c.ackPacket.setSequence(c.remoteSequence)
		c.peer.sendUserData(c.ackPacket)
	}
}

func (c *sequencedChannel) processAck(p *packet) {
	if c.lastPacket != nil && p.sequence() == c.lastPacket.sequence() {
		c.lastPacket = nil
	}
}

func (c *sequencedChannel) processPacket(p *packet) {
	relative := relativeSequenceNumber(int(p.sequence()), int(c.remoteSequence))
	if p.sequence() < MaxSequence && relative > 0 {
		c.peer.stats.PacketLoss += uint64(relative - 1)
		c.remoteSequence = p.sequence()
		c.peer.addIncomingPacket(p)
	}
	c.mustSendAck = true
}

// dequeue drops the first n packets of q, keeping its storage.
func dequeue(q []*packet, n int) []*packet {
	m := copy(q, q[n:])
	clear(q[m:])
	return q[:m]
}
