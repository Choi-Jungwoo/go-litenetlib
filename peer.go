package litenetlib

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionState is a peer connection state; values are flags (NetPeer.cs).
type ConnectionState byte

const (
	StateIncoming          ConnectionState = 1 << 1
	StateConnected         ConnectionState = 1 << 3
	StateShutdownRequested ConnectionState = 1 << 4
	StateDisconnected      ConnectionState = 1 << 5
	// StateAny matches every live state (1<<2 is the client-only Outcoming state).
	StateAny = StateIncoming | 1<<2 | StateConnected | StateShutdownRequested
)

// ErrTooBigPacket is returned by Send when data cannot be sent with the chosen method.
var ErrTooBigPacket = errors.New("litenetlib: packet too big")

type connectRequestResult byte

const (
	connectNone connectRequestResult = iota
	connectReconnection
	connectNewConnection
)

const (
	mtuCheckDelay       = 1000
	maxMtuCheckAttempts = 4
	shutdownDelay       = 300
)

// Statistics are connection counters (NetStatistics.cs).
type Statistics struct {
	PacketsSent     uint64
	PacketsReceived uint64
	BytesSent       uint64
	BytesReceived   uint64
	PacketLoss      uint64
}

// String formats the counters like NetStatistics.ToString.
func (s Statistics) String() string {
	return fmt.Sprintf("BytesReceived: %d\nPacketsReceived: %d\nBytesSent: %d\nPacketsSent: %d\nPacketLoss: %d\nPacketLossPercent: %d\n",
		s.BytesReceived, s.PacketsReceived, s.BytesSent, s.PacketsSent, s.PacketLoss, s.PacketLossPercent())
}

// PacketLossPercent is PacketLoss relative to PacketsSent.
func (s Statistics) PacketLossPercent() uint64 {
	if s.PacketsSent == 0 {
		return 0
	}
	return s.PacketLoss * 100 / s.PacketsSent
}

type incomingFragments struct {
	fragments     []*packet
	receivedCount int
	totalSize     int
}

// stopwatch mirrors the System.Diagnostics.Stopwatch semantics used for pings.
type stopwatch struct {
	start   time.Time
	elapsed time.Duration
	running bool
}

func (s *stopwatch) restart() { *s = stopwatch{start: time.Now(), running: true} }
func (s *stopwatch) stop() {
	if s.running {
		s.elapsed = time.Since(s.start)
		s.running = false
	}
}
func (s *stopwatch) elapsedMs() int {
	if s.running {
		return int(time.Since(s.start).Milliseconds())
	}
	return int(s.elapsed.Milliseconds())
}

// Peer is a remote connection; its main purpose is sending messages to that client.
type Peer struct {
	// Tag is application-defined data about the connection.
	Tag any

	m          *Manager
	addr       netip.AddrPort
	id         int
	next, prev *Peer // guarded by m.peersMu

	state atomic.Uint32
	mtu   atomic.Int32

	// queueMu guards the outgoing queues and fragmentID.
	queueMu         sync.Mutex
	unreliableQueue []*packet
	fragmentID      uint16

	// mu guards all protocol state below.
	mu                  sync.Mutex
	rtt                 int
	avgRtt              int
	rttCount            int
	resendDelay         float64
	pingSendTimer       int
	rttResetTimer       int
	pingTimer           stopwatch
	timeSinceLastPacket int
	remoteDelta         int64

	reliableOrdered   reliableChannel
	reliableUnordered reliableChannel
	sequenced         sequencedChannel
	reliableSequenced sequencedChannel

	mtuIdx           int
	finishMtu        bool
	mtuCheckTimer    int
	mtuCheckAttempts int

	holdedFragments map[uint16]*incomingFragments

	mergeData  *packet
	mergePos   int
	mergeCount int

	connectTime         int64
	connectNum          byte
	shutdownPacket      *packet
	shutdownTimer       int
	pingPacket          *packet
	pongPacket          *packet
	connectAcceptPacket *packet
	stats               Statistics
}

func newPeer(m *Manager, addr netip.AddrPort, id int) *Peer {
	p := &Peer{
		m:               m,
		addr:            addr,
		id:              id,
		resendDelay:     27.0,
		holdedFragments: make(map[uint16]*incomingFragments),
		mergeData:       newPacket(propMerged, maxPacketSize),
		pongPacket:      newPacket(propPong, 0),
		pingPacket:      newPacket(propPing, 0),
	}
	p.state.Store(uint32(StateIncoming))
	p.mtu.Store(int32(possibleMtu[0]))
	p.pingPacket.setSequence(1)
	p.reliableOrdered.init(p, true)
	p.reliableUnordered.init(p, false)
	p.sequenced.init(p, false)
	p.reliableSequenced.init(p, true)
	return p
}

// ID can be used as a key for peers; IDs of removed peers are reused.
func (p *Peer) ID() int { return p.id }

// EndPoint is the peer's address.
func (p *Peer) EndPoint() netip.AddrPort { return p.addr }

// ConnectionState is the current connection state.
func (p *Peer) ConnectionState() ConnectionState { return ConnectionState(p.state.Load()) }

func (p *Peer) setState(s ConnectionState) { p.state.Store(uint32(s)) }

// Ping is the current ping in milliseconds.
func (p *Peer) Ping() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.avgRtt / 2
}

// Mtu is the current maximum UDP payload size without fragmentation.
func (p *Peer) Mtu() int { return int(p.mtu.Load()) }

// RemoteTimeDelta is the (inaccurate) remote clock offset in .NET ticks; positive if the remote clock is ahead.
func (p *Peer) RemoteTimeDelta() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.remoteDelta
}

// RemoteUTCTime is the (inaccurate) remote UTC time.
func (p *Peer) RemoteUTCTime() time.Time {
	return time.Now().UTC().Add(time.Duration(p.RemoteTimeDelta()) * 100)
}

// TimeSinceLastPacket is the time in milliseconds since any packet was received from the peer.
func (p *Peer) TimeSinceLastPacket() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.timeSinceLastPacket
}

// Manager is the peer's parent manager.
func (p *Peer) Manager() *Manager { return p.m }

// PacketsCountInReliableQueue is the number of queued ReliableUnordered packets.
func (p *Peer) PacketsCountInReliableQueue() int {
	p.queueMu.Lock()
	defer p.queueMu.Unlock()
	return len(p.reliableUnordered.queue) - p.reliableUnordered.queueHead
}

// PacketsCountInReliableOrderedQueue is the number of queued ReliableOrdered packets.
func (p *Peer) PacketsCountInReliableOrderedQueue() int {
	p.queueMu.Lock()
	defer p.queueMu.Unlock()
	return len(p.reliableOrdered.queue) - p.reliableOrdered.queueHead
}

// Statistics returns a snapshot of the connection counters.
func (p *Peer) Statistics() Statistics {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// ResetStatistics zeroes the connection counters.
func (p *Peer) ResetStatistics() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stats = Statistics{}
}

func (p *Peer) setConnectionNum(n byte) {
	p.connectNum = n
	p.mergeData.setConnectionNumber(n)
	p.pingPacket.setConnectionNumber(n)
	p.pongPacket.setConnectionNumber(n)
}

func (p *Peer) currentConnectTime() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.connectTime
}

func (p *Peer) accept(connectID int64, connectNum byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connectTime = connectID
	p.setState(StateConnected)
	p.setConnectionNum(connectNum)
	p.connectAcceptPacket = newPacket(propConnectAccept, 0)
	binary.LittleEndian.PutUint64(p.connectAcceptPacket.data[1:], uint64(connectID))
	p.connectAcceptPacket.data[9] = connectNum
	p.connectAcceptPacket.data[10] = 0 // not a reused peer
	p.sendRaw(p.connectAcceptPacket.data)
}

func (p *Peer) reject(connectID int64, connectNum byte, data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connectTime = connectID
	p.connectNum = connectNum
	p.shutdown(data, false)
}

func sendOptionsToProperty(method DeliveryMethod) packetProperty {
	switch method {
	case ReliableUnordered:
		return propReliableUnordered
	case Sequenced:
		return propSequenced
	case ReliableOrdered:
		return propReliableOrdered
	case ReliableSequenced:
		return propReliableSequenced
	default:
		return propUnreliable
	}
}

// GetMaxSinglePacketSize is the largest payload that is sent without fragmentation.
func (p *Peer) GetMaxSinglePacketSize(method DeliveryMethod) int {
	return p.Mtu() - propertyHeaderSize(sendOptionsToProperty(method))
}

// Send queues data for the peer. Payloads above GetMaxSinglePacketSize are fragmented for
// reliable methods; other methods return ErrTooBigPacket.
func (p *Peer) Send(data []byte, method DeliveryMethod) error {
	if s := p.ConnectionState(); s == StateShutdownRequested || s == StateDisconnected {
		return nil
	}
	prop := sendOptionsToProperty(method)
	var queue *[]*packet
	switch prop {
	case propReliableUnordered:
		queue = &p.reliableUnordered.queue
	case propSequenced:
		queue = &p.sequenced.queue
	case propReliableOrdered:
		queue = &p.reliableOrdered.queue
	case propReliableSequenced:
		queue = &p.reliableSequenced.queue
	default:
		queue = &p.unreliableQueue
	}

	hs := propertyHeaderSize(prop)
	mtu := p.Mtu()
	if len(data)+hs <= mtu {
		pk := p.m.pool.getWithData(prop, data)
		p.queueMu.Lock()
		*queue = append(*queue, pk)
		p.queueMu.Unlock()
		return nil
	}
	if prop == propSequenced || prop == propUnreliable || prop == propReliableSequenced {
		return ErrTooBigPacket
	}

	packetFullSize := mtu - hs
	packetDataSize := packetFullSize - FragmentHeaderSize
	fullPackets := len(data) / packetDataSize
	lastSize := len(data) % packetDataSize
	total := fullPackets
	if lastSize != 0 {
		total++
	}
	if total > 0xFFFF {
		return ErrTooBigPacket
	}
	dataOffset := hs + FragmentHeaderSize

	p.queueMu.Lock()
	defer p.queueMu.Unlock()
	for i := 0; i < total; i++ {
		chunk := data[i*packetDataSize : min(len(data), (i+1)*packetDataSize)]
		pk := p.m.pool.getWithProperty(prop, len(chunk)+FragmentHeaderSize)
		pk.setFragment(p.fragmentID, uint16(i), uint16(total))
		pk.markFragmented()
		copy(pk.data[dataOffset:], chunk)
		*queue = append(*queue, pk)
	}
	p.fragmentID++
	return nil
}

// Disconnect gracefully disconnects the peer, sending data (may be nil) along.
func (p *Peer) Disconnect(data []byte) { p.m.DisconnectPeer(p, data) }

// remoteDisconnect disconnects the peer if pk is its valid disconnect packet (NetPeer.ProcessDisconnect).
func (p *Peer) remoteDisconnect(pk *packet) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.isDisconnectPacket(pk) {
		return false
	}
	p.m.disconnectPeerLocked(p, RemoteConnectionClose, nil, true, nil, pk)
	return true
}

func (p *Peer) isDisconnectPacket(pk *packet) bool {
	return p.ConnectionState() == StateConnected &&
		len(pk.data) >= 9 &&
		int64(binary.LittleEndian.Uint64(pk.data[1:])) == p.connectTime &&
		pk.connectionNumber() == p.connectNum
}

// shutdown mirrors NetPeer.Shutdown; it reports false if already shutting down.
func (p *Peer) shutdown(data []byte, force bool) bool {
	if s := p.ConnectionState(); s == StateDisconnected || s == StateShutdownRequested {
		return false
	}
	if force {
		p.setState(StateDisconnected)
		return true
	}
	p.timeSinceLastPacket = 0 // reconnect protection

	p.shutdownPacket = newPacket(propDisconnect, len(data))
	p.shutdownPacket.setConnectionNumber(p.connectNum)
	binary.LittleEndian.PutUint64(p.shutdownPacket.data[1:], uint64(p.connectTime))
	if len(p.shutdownPacket.data) >= p.Mtu() {
		writeError("[Peer] Disconnect additional data size more than MTU - 8!")
	} else {
		copy(p.shutdownPacket.data[9:], data)
	}
	p.setState(StateShutdownRequested)
	p.sendRaw(p.shutdownPacket.data)
	return true
}

func (p *Peer) updateRoundTripTime(rtt int) {
	p.rtt += rtt
	p.rttCount++
	p.avgRtt = p.rtt / p.rttCount
	p.resendDelay = 25.0 + float64(p.avgRtt)*2.1 // 25 ms + double rtt
}

func (p *Peer) addIncomingPacket(pk *packet) {
	if !pk.isFragmented() {
		p.m.receiveFromPeer(p, pk)
		return
	}
	id := pk.fragmentID()
	inc := p.holdedFragments[id]
	if inc == nil {
		inc = &incomingFragments{fragments: make([]*packet, pk.fragmentsTotal())}
		p.holdedFragments[id] = inc
	}
	part := int(pk.fragmentPart())
	if part >= len(inc.fragments) || inc.fragments[part] != nil {
		p.m.pool.recycle(pk)
		writeError("Invalid fragment packet")
		return
	}
	inc.fragments[part] = pk
	inc.receivedCount++
	dataOffset := pk.headerSize() + FragmentHeaderSize
	inc.totalSize += len(pk.data) - dataOffset
	if inc.receivedCount != len(inc.fragments) {
		return
	}

	result := p.m.pool.getWithProperty(pk.property(), inc.totalSize)
	offset := result.headerSize()
	firstSize := len(inc.fragments[0].data) - dataOffset
	for i, f := range inc.fragments {
		// Inconsistent fragment sizes panic here like Buffer.BlockCopy throws in the reference.
		copy(result.data[offset+firstSize*i:offset+firstSize*i+len(f.data)-dataOffset], f.data[dataOffset:])
		p.m.pool.recycle(f)
		inc.fragments[i] = nil
	}
	p.m.receiveFromPeer(p, result)
	delete(p.holdedFragments, id)
}

func (p *Peer) processMtuPacket(pk *packet) {
	if len(pk.data) < possibleMtu[0] {
		return
	}
	receivedMtu := int(int32(binary.LittleEndian.Uint32(pk.data[1:])))
	endMtuCheck := int(int32(binary.LittleEndian.Uint32(pk.data[len(pk.data)-4:])))
	if receivedMtu != len(pk.data) || receivedMtu != endMtuCheck || receivedMtu > maxPacketSize {
		writeError("[MTU] Broken packet. RMTU %d, EMTU %d, PSIZE %d", receivedMtu, endMtuCheck, len(pk.data))
		return
	}

	if pk.property() == propMtuCheck {
		p.mtuCheckAttempts = 0
		pk.setProperty(propMtuOk)
		p.sendRaw(pk.data)
		p.m.pool.recycle(pk)
	} else if receivedMtu > p.Mtu() && !p.finishMtu {
		if receivedMtu != possibleMtu[p.mtuIdx+1] {
			return
		}
		p.mtuIdx++
		p.mtu.Store(int32(receivedMtu))
		if p.mtuIdx == len(possibleMtu)-1 {
			p.finishMtu = true
		}
	}
}

func (p *Peer) updateMtuLogic(delta int) {
	if p.finishMtu {
		return
	}
	p.mtuCheckTimer += delta
	if p.mtuCheckTimer < mtuCheckDelay {
		return
	}
	p.mtuCheckTimer = 0
	p.mtuCheckAttempts++
	if p.mtuCheckAttempts >= maxMtuCheckAttempts {
		p.finishMtu = true
		return
	}
	if p.mtuIdx >= len(possibleMtu)-1 {
		return
	}
	// The probe keeps connection number 0, exactly like the reference.
	newMtu := possibleMtu[p.mtuIdx+1]
	pk := p.m.pool.getWithProperty(propMtuCheck, newMtu-HeaderSize)
	binary.LittleEndian.PutUint32(pk.data[1:], uint32(newMtu))
	binary.LittleEndian.PutUint32(pk.data[len(pk.data)-4:], uint32(newMtu))
	if p.sendRaw(pk.data) <= 0 {
		p.finishMtu = true
	}
	p.m.pool.recycle(pk)
}

// processConnectRequest mirrors NetPeer.ProcessConnectRequest; it also returns the current connection number.
func (p *Peer) processConnectRequest(connectTime int64, connectNum byte) (connectRequestResult, byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch p.ConnectionState() {
	case StateIncoming:
		if connectTime >= p.connectTime {
			p.connectTime = connectTime
			p.setConnectionNum(connectNum)
		}
	case StateConnected:
		if connectTime == p.connectTime {
			p.sendRaw(p.connectAcceptPacket.data) // old request: just reply accept
		} else if connectTime > p.connectTime {
			return connectReconnection, p.connectNum
		}
	case StateDisconnected, StateShutdownRequested:
		if connectTime >= p.connectTime {
			return connectNewConnection, p.connectNum
		}
	}
	return connectNone, p.connectNum
}

func (p *Peer) processPacket(pk *packet) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.processPacketLocked(pk)
}

func (p *Peer) processPacketLocked(pk *packet) {
	if p.ConnectionState() == StateIncoming {
		p.m.pool.recycle(pk) // not initialized
		return
	}
	if pk.connectionNumber() != p.connectNum && pk.property() != propShutdownOk {
		p.m.pool.recycle(pk) // old packet
		return
	}
	p.timeSinceLastPacket = 0

	switch pk.property() {
	case propMerged:
		for pos := HeaderSize; pos < len(pk.data); {
			// Truncated entries stop processing where the reference would throw.
			if pos+2 > len(pk.data) {
				break
			}
			size := int(binary.LittleEndian.Uint16(pk.data[pos:]))
			pos += 2
			if pos+size > len(pk.data) {
				break
			}
			merged := p.m.pool.get(size, false)
			if !merged.fromBytes(pk.data[pos : pos+size]) {
				p.m.pool.recycle(merged)
				break
			}
			pos += size
			p.processPacketLocked(merged)
		}
		p.m.pool.recycle(pk)

	case propPing:
		if relativeSequenceNumber(int(pk.sequence()), int(p.pongPacket.sequence())) > 0 {
			binary.LittleEndian.PutUint64(p.pongPacket.data[3:], uint64(utcTicks()))
			p.pongPacket.setSequence(pk.sequence())
			p.sendRaw(p.pongPacket.data)
		}
		p.m.pool.recycle(pk)

	case propPong:
		if pk.sequence() == p.pingPacket.sequence() {
			p.pingTimer.stop()
			elapsed := p.pingTimer.elapsedMs()
			p.remoteDelta = int64(binary.LittleEndian.Uint64(pk.data[3:])) + int64(elapsed)*ticksPerMillisecond/2 - utcTicks()
			p.updateRoundTripTime(elapsed)
			p.m.connectionLatencyUpdated(p, elapsed/2)
		}
		p.m.pool.recycle(pk)

	case propAckReliable:
		p.reliableUnordered.processAck(pk)
		p.m.pool.recycle(pk)
	case propAckReliableOrdered:
		p.reliableOrdered.processAck(pk)
		p.m.pool.recycle(pk)
	case propAckReliableSequenced:
		p.reliableSequenced.processAck(pk)
		p.m.pool.recycle(pk)
	case propSequenced:
		p.sequenced.processPacket(pk)
	case propReliableUnordered:
		p.reliableUnordered.processPacket(pk)
	case propReliableOrdered:
		p.reliableOrdered.processPacket(pk)
	case propReliableSequenced:
		p.reliableSequenced.processPacket(pk)
	case propUnreliable:
		p.addIncomingPacket(pk)
	case propMtuCheck, propMtuOk:
		p.processMtuPacket(pk)
	case propShutdownOk:
		if p.ConnectionState() == StateShutdownRequested {
			p.setState(StateDisconnected)
		}
		p.m.pool.recycle(pk)
	default:
		writeError("Error! Unexpected packet type: %d", pk.property())
	}
}

func (p *Peer) sendMerged() {
	if p.mergeCount == 0 {
		return
	}
	var n int
	if p.mergeCount > 1 {
		n = p.sendRaw(p.mergeData.data[:HeaderSize+p.mergePos])
	} else {
		// single packet: send without merge header and length
		n = p.sendRaw(p.mergeData.data[HeaderSize+2 : HeaderSize+p.mergePos])
	}
	p.countSent(n)
	p.mergePos = 0
	p.mergeCount = 0
}

func (p *Peer) countSent(n int) {
	if debugBuild {
		p.stats.PacketsSent++
		p.stats.BytesSent += uint64(n) // a failed send (-1) wraps, like (ulong)bytesSent
	}
}

func (p *Peer) sendUserData(pk *packet) {
	pk.setConnectionNumber(p.connectNum)
	mergedPacketSize := HeaderSize + len(pk.data) + 2
	const sizeThreshold = 20
	mtu := p.Mtu()
	if mergedPacketSize+sizeThreshold >= mtu {
		p.countSent(p.sendRaw(pk.data))
		return
	}
	if p.mergePos+mergedPacketSize > mtu {
		p.sendMerged()
	}
	binary.LittleEndian.PutUint16(p.mergeData.data[p.mergePos+HeaderSize:], uint16(len(pk.data)))
	copy(p.mergeData.data[p.mergePos+HeaderSize+2:], pk.data)
	p.mergePos += len(pk.data) + 2
	p.mergeCount++
}

// Flush sends all queued packets now instead of waiting for the next update.
func (p *Peer) Flush() {
	if p.ConnectionState() != StateConnected {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.flushLocked()
}

func (p *Peer) flushLocked() {
	p.reliableOrdered.sendNextPackets()
	p.reliableUnordered.sendNextPackets()
	p.reliableSequenced.sendNextPackets()
	p.sequenced.sendNextPackets()
	p.queueMu.Lock()
	for _, pk := range p.unreliableQueue {
		p.sendUserData(pk)
		p.m.pool.recycle(pk)
	}
	clear(p.unreliableQueue)
	p.unreliableQueue = p.unreliableQueue[:0]
	p.queueMu.Unlock()
	p.sendMerged()
}

// update runs one logic tick. It reports whether the peer is disconnected long enough
// to be removed, plus its packet loss for the manager statistics.
func (p *Peer) update(delta int) (remove bool, packetLoss uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	timeout := p.m.DisconnectTimeout
	if p.ConnectionState() == StateDisconnected && p.timeSinceLastPacket > timeout {
		return true, 0
	}
	p.timeSinceLastPacket += delta
	switch p.ConnectionState() {
	case StateConnected:
		if p.timeSinceLastPacket > timeout {
			p.m.disconnectPeerLocked(p, Timeout, nil, true, nil, nil)
			return false, p.stats.PacketLoss
		}
	case StateShutdownRequested:
		if p.timeSinceLastPacket > timeout {
			p.setState(StateDisconnected)
		} else {
			p.shutdownTimer += delta
			if p.shutdownTimer >= shutdownDelay {
				p.shutdownTimer = 0
				p.sendRaw(p.shutdownPacket.data)
			}
		}
		return false, p.stats.PacketLoss
	default:
		return false, p.stats.PacketLoss
	}

	p.pingSendTimer += delta
	if p.pingSendTimer >= p.m.PingInterval {
		p.pingSendTimer = 0
		p.pingPacket.setSequence(p.pingPacket.sequence() + 1)
		if p.pingTimer.running { // ping timeout
			p.updateRoundTripTime(p.pingTimer.elapsedMs())
		}
		p.pingTimer.restart()
		p.sendRaw(p.pingPacket.data)
	}

	p.rttResetTimer += delta
	if p.rttResetTimer >= p.m.PingInterval*3 {
		p.rttResetTimer = 0
		p.rtt = p.avgRtt
		p.rttCount = 1
	}

	p.updateMtuLogic(delta)
	p.flushLocked()
	return false, p.stats.PacketLoss
}

// sendRaw sends with p.mu held; socket errors that end the connection disconnect the peer.
func (p *Peer) sendRaw(data []byte) int {
	n, reason, err := p.m.sendRaw(data, p.addr)
	if reason != noDisconnect {
		p.m.disconnectPeerLocked(p, reason, err, true, nil, nil)
		p.m.sendFailed(reason, p.addr, err)
	}
	return n
}
