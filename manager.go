package litenetlib

import (
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ticksPerMillisecond = 10000
	unixEpochTicks      = 621355968000000000
)

// utcTicks is DateTime.UtcNow.Ticks.
func utcTicks() int64 { return time.Now().UnixNano()/100 + unixEpochTicks }

// ErrAlreadyRunning is returned by Start on a running manager.
var ErrAlreadyRunning = errors.New("litenetlib: manager already running")

// Manager is the server: it owns the socket, the peers and the logic loop (NetManager.cs).
// Configuration fields must be set before Start.
type Manager struct {
	// UnconnectedMessagesEnabled enables receiving unconnected messages.
	UnconnectedMessagesEnabled bool
	// NatPunchEnabled enables NAT punch messages.
	NatPunchEnabled bool
	// UpdateTime is the logic update and send period in milliseconds.
	UpdateTime int
	// PingInterval is the latency detection and keepalive interval in milliseconds.
	PingInterval int
	// DisconnectTimeout closes a connection when nothing is received for this many milliseconds.
	DisconnectTimeout int
	// UnsyncedEvents delivers events from a library goroutine instead of PollEvents.
	UnsyncedEvents bool
	// DiscoveryEnabled enables receiving discovery requests.
	DiscoveryEnabled bool
	// ReuseAddress sets SO_REUSEADDR on the sockets.
	ReuseAddress bool
	// AutoRecycle recycles PacketReaders automatically after the receive callback.
	AutoRecycle bool
	// NatPunchModule performs NAT hole punching.
	NatPunchModule *NatPunchModule
	// SimulatePacketLoss drops SimulationPacketLossChance percent of incoming datagrams
	// (DEBUG-only in the reference).
	SimulatePacketLoss bool
	// SimulateLatency holds incoming datagrams for SimulationMinLatency to
	// SimulationMaxLatency milliseconds (DEBUG-only in the reference).
	SimulateLatency            bool
	SimulationPacketLossChance int
	SimulationMinLatency       int
	SimulationMaxLatency       int

	listener EventListener

	runMu     sync.Mutex
	running   atomic.Bool
	localPort atomic.Int32
	conn4     *net.UDPConn
	conn6     *net.UDPConn
	stop      chan struct{}
	wg        sync.WaitGroup

	peersMu    sync.RWMutex
	peers      map[netip.AddrPort]*Peer
	headPeer   *Peer
	lastPeerID int
	peerIDs    []int

	connectedPeers atomic.Int32

	eventsMu    sync.Mutex
	events      []*netEvent
	eventsHead  int
	eventSignal chan struct{}

	simMu   sync.Mutex
	delayed []delayedDatagram

	packetsSent     atomic.Uint64
	packetsReceived atomic.Uint64
	bytesSent       atomic.Uint64
	bytesReceived   atomic.Uint64
	packetLoss      atomic.Uint64
}

// NewManager creates a manager delivering events to listener.
func NewManager(listener EventListener) *Manager {
	m := &Manager{
		UpdateTime:        15,
		PingInterval:      1000,
		DisconnectTimeout: 5000,

		SimulationPacketLossChance: 10,
		SimulationMinLatency:       30,
		SimulationMaxLatency:       100,

		listener: listener,
		peers:    make(map[netip.AddrPort]*Peer),
	}
	m.NatPunchModule = &NatPunchModule{m: m}
	return m
}

// IsRunning reports whether the socket is listening and the logic loop runs.
func (m *Manager) IsRunning() bool { return m.running.Load() }

// LocalPort is the bound UDP port.
func (m *Manager) LocalPort() int { return int(m.localPort.Load()) }

// Start listens on port on all IPv4 and IPv6 addresses; port 0 picks a free port.
func (m *Manager) Start(port int) error {
	return m.StartAddr(netip.IPv4Unspecified(), netip.IPv6Unspecified(), port)
}

// StartAddr listens on the given IPv4 and IPv6 addresses. IPv6 is best effort, as in the reference.
func (m *Manager) StartAddr(addr4, addr6 netip.Addr, port int) error {
	m.runMu.Lock()
	defer m.runMu.Unlock()
	if m.running.Load() {
		return ErrAlreadyRunning
	}
	conn4, err := listenUDP("udp4", netip.AddrPortFrom(addr4, uint16(port)), m.ReuseAddress)
	if err != nil {
		writeError("[B]Bind exception: %v", err)
		return err
	}
	localPort := uint16(conn4.LocalAddr().(*net.UDPAddr).Port)
	conn6, _ := listenUDP("udp6", netip.AddrPortFrom(addr6, localPort), m.ReuseAddress)

	m.conn4, m.conn6 = conn4, conn6
	m.localPort.Store(int32(localPort))
	m.stop = make(chan struct{})
	m.eventSignal = make(chan struct{}, 1)
	m.running.Store(true)
	if m.UnsyncedEvents {
		m.wg.Add(1)
		go m.dispatchEvents()
	}
	m.wg.Add(2)
	go m.receiveLoop(conn4)
	go m.updateLogic()
	if conn6 != nil {
		m.wg.Add(1)
		go m.receiveLoop(conn6)
	}
	return nil
}

// Stop closes all connections and stops the manager. With sendDisconnectMessages the
// peers are told about the shutdown; no disconnect events are raised either way.
func (m *Manager) Stop(sendDisconnectMessages bool) {
	m.runMu.Lock()
	defer m.runMu.Unlock()
	if !m.running.Load() {
		return
	}
	for _, p := range m.snapshotPeers(nil) {
		p.mu.Lock()
		p.shutdown(nil, !sendDisconnectMessages)
		p.mu.Unlock()
	}
	m.running.Store(false)
	close(m.stop)
	m.conn4.Close()
	if m.conn6 != nil {
		m.conn6.Close()
	}
	m.wg.Wait()

	m.peersMu.Lock()
	m.headPeer = nil
	clear(m.peers)
	m.peersMu.Unlock()
	m.connectedPeers.Store(0)
	m.simMu.Lock()
	m.delayed = nil
	m.simMu.Unlock()
	m.eventsMu.Lock()
	clear(m.events)
	m.events, m.eventsHead = m.events[:0], 0
	m.eventsMu.Unlock()
}

// snapshotPeers appends the peers, newest first, to dst.
func (m *Manager) snapshotPeers(dst []*Peer) []*Peer {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()
	for p := m.headPeer; p != nil; p = p.next {
		dst = append(dst, p)
	}
	return dst
}

// Peers appends the peers whose state matches the state flags to dst, newest first.
func (m *Manager) Peers(dst []*Peer, state ConnectionState) []*Peer {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()
	for p := m.headPeer; p != nil; p = p.next {
		if p.ConnectionState()&state != 0 {
			dst = append(dst, p)
		}
	}
	return dst
}

// PeersCount is the number of connected peers.
func (m *Manager) PeersCount() int { return int(m.connectedPeers.Load()) }

// PeersCountInState counts the peers whose state matches the state flags.
func (m *Manager) PeersCountInState(state ConnectionState) int {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()
	n := 0
	for p := m.headPeer; p != nil; p = p.next {
		if p.ConnectionState()&state != 0 {
			n++
		}
	}
	return n
}

// Statistics returns a snapshot of the counters of all connections.
func (m *Manager) Statistics() Statistics {
	return Statistics{
		PacketsSent:     m.packetsSent.Load(),
		PacketsReceived: m.packetsReceived.Load(),
		BytesSent:       m.bytesSent.Load(),
		BytesReceived:   m.bytesReceived.Load(),
		PacketLoss:      m.packetLoss.Load(),
	}
}

// ResetStatistics zeroes the counters of all connections.
func (m *Manager) ResetStatistics() {
	m.packetsSent.Store(0)
	m.packetsReceived.Store(0)
	m.bytesSent.Store(0)
	m.bytesReceived.Store(0)
	m.packetLoss.Store(0)
}

// FirstPeer is the most recently added peer, or nil.
func (m *Manager) FirstPeer() *Peer {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()
	return m.headPeer
}

func (m *Manager) tryGetPeer(addr netip.AddrPort) *Peer {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()
	return m.peers[addr]
}

// tryAddPeer adds peer unless one exists for its address, which is returned instead.
func (m *Manager) tryAddPeer(peer *Peer) *Peer {
	m.peersMu.Lock()
	defer m.peersMu.Unlock()
	if existing := m.peers[peer.addr]; existing != nil {
		m.peerIDs = append(m.peerIDs, peer.id)
		return existing
	}
	if m.headPeer != nil {
		peer.next = m.headPeer
		m.headPeer.prev = peer
	}
	m.headPeer = peer
	m.peers[peer.addr] = peer
	return peer
}

func (m *Manager) removePeer(peer *Peer) {
	m.peersMu.Lock()
	defer m.peersMu.Unlock()
	m.removePeerLocked(peer)
}

func (m *Manager) removePeerLocked(peer *Peer) {
	if m.peers[peer.addr] != peer {
		return
	}
	delete(m.peers, peer.addr)
	if peer == m.headPeer {
		m.headPeer = peer.next
	}
	if peer.prev != nil {
		peer.prev.next = peer.next
	}
	if peer.next != nil {
		peer.next.prev = peer.prev
	}
	peer.prev, peer.next = nil, nil
	m.peerIDs = append(m.peerIDs, peer.id)
}

func (m *Manager) nextPeerID() int {
	m.peersMu.Lock()
	defer m.peersMu.Unlock()
	if len(m.peerIDs) == 0 {
		id := m.lastPeerID
		m.lastPeerID++
		return id
	}
	id := m.peerIDs[0]
	m.peerIDs = m.peerIDs[1:]
	return id
}

// noDisconnect is the sendRaw result when the peer stays connected.
const noDisconnect DisconnectReason = 0xFF

// sendRaw writes one datagram. It never takes peer locks; the caller handles the returned
// disconnect reason (noDisconnect when the peer should stay).
func (m *Manager) sendRaw(data []byte, addr netip.AddrPort) (int, DisconnectReason, error) {
	if !m.running.Load() {
		return 0, noDisconnect, nil
	}
	conn := m.conn4
	if m.conn6 != nil && addr.Addr().Is6() && !addr.Addr().Is4In6() {
		conn = m.conn6
	}
	n, err := conn.WriteToUDPAddrPort(data, addr)
	if err != nil {
		switch {
		case errors.Is(err, errMessageSize):
			return -1, noDisconnect, nil
		case errors.Is(err, errHostUnreachable):
			writeError("[S]%v", err)
			return -1, HostUnreachable, err
		case errors.Is(err, errConnectionReset):
			writeError("[S]%v", err)
			return -1, RemoteConnectionClose, err
		}
		if !errors.Is(err, errNoBufferSpace) {
			writeError("[S]%v", err)
		}
		return 0, noDisconnect, nil
	}
	m.packetsSent.Add(1)
	m.bytesSent.Add(uint64(len(data)))
	return n, noDisconnect, nil
}

// sendRawUnlocked sends without holding any peer lock, disconnecting the addressed peer on fatal errors.
func (m *Manager) sendRawUnlocked(data []byte, addr netip.AddrPort) int {
	n, reason, err := m.sendRaw(data, addr)
	if reason != noDisconnect {
		if p := m.tryGetPeer(addr); p != nil {
			m.disconnectPeer(p, reason, err, true, nil, nil)
		}
		m.sendFailed(reason, addr, err)
	}
	return n
}

// sendFailed raises the error event that follows a HostUnreachable disconnect.
func (m *Manager) sendFailed(reason DisconnectReason, addr netip.AddrPort, err error) {
	if reason == HostUnreachable {
		m.pushEvent(m.newEvent(evError, nil, addr, err))
	}
}

func (m *Manager) sendAndRecycle(pk *packet, addr netip.AddrPort) int {
	n := m.sendRawUnlocked(pk.data, addr)
	recyclePacket(pk)
	return n
}

func (m *Manager) disconnectPeer(p *Peer, reason DisconnectReason, sockErr error, force bool, data []byte, eventData *packet) {
	p.mu.Lock()
	defer p.mu.Unlock()
	m.disconnectPeerLocked(p, reason, sockErr, force, data, eventData)
}

func (m *Manager) disconnectPeerLocked(p *Peer, reason DisconnectReason, sockErr error, force bool, data []byte, eventData *packet) {
	wasConnected := p.ConnectionState() == StateConnected
	if !p.shutdown(data, force) {
		return
	}
	if wasConnected {
		m.connectedPeers.Add(-1)
	}
	evt := m.newEvent(evDisconnect, p, netip.AddrPort{}, sockErr)
	evt.reason = reason
	evt.setSource(eventData)
	m.pushEvent(evt)
}

// DisconnectPeer gracefully disconnects peer, sending data (may be nil; at most MTU-9 bytes).
func (m *Manager) DisconnectPeer(peer *Peer, data []byte) {
	m.disconnectPeer(peer, DisconnectPeerCalled, nil, false, data, nil)
}

// DisconnectPeerForce disconnects peer immediately without notifying it.
func (m *Manager) DisconnectPeerForce(peer *Peer) {
	m.disconnectPeer(peer, DisconnectPeerCalled, nil, true, nil, nil)
}

// DisconnectAll gracefully disconnects all peers, sending data (may be nil).
func (m *Manager) DisconnectAll(data []byte) {
	for _, p := range m.snapshotPeers(nil) {
		m.disconnectPeer(p, DisconnectPeerCalled, nil, false, data, nil)
	}
}

// SendToAll sends data to all peers.
func (m *Manager) SendToAll(data []byte, method DeliveryMethod) error {
	return m.SendToAllExcept(data, method, nil)
}

// SendToAllExcept sends data to all peers except excludePeer.
func (m *Manager) SendToAllExcept(data []byte, method DeliveryMethod, excludePeer *Peer) error {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()
	for p := m.headPeer; p != nil; p = p.next {
		if p != excludePeer {
			if err := p.Send(data, method); err != nil {
				return err
			}
		}
	}
	return nil
}

// Flush sends the queued packets of all peers now.
func (m *Manager) Flush() {
	for _, p := range m.snapshotPeers(nil) {
		p.Flush()
	}
}

// SendUnconnectedMessage sends data to addr without a connection.
func (m *Manager) SendUnconnectedMessage(data []byte, addr netip.AddrPort) bool {
	return m.sendUnconnected(propUnconnectedMessage, data, addr)
}

// SendDiscoveryResponse answers a discovery request from addr.
func (m *Manager) SendDiscoveryResponse(data []byte, addr netip.AddrPort) bool {
	return m.sendUnconnected(propDiscoveryResponse, data, addr)
}

func (m *Manager) sendUnconnected(prop packetProperty, data []byte, addr netip.AddrPort) bool {
	if !m.running.Load() {
		return false
	}
	return m.sendAndRecycle(getWithData(prop, data), addr) > 0
}

func (m *Manager) updateLogic() {
	defer m.wg.Done()
	var peers []*Peer
	var toRemove []*Peer
	timer := time.NewTimer(0)
	<-timer.C
	last := time.Now()
	for m.running.Load() {
		if m.SimulateLatency {
			m.releaseDelayed()
		}
		elapsed := int(time.Since(last).Milliseconds())
		if elapsed <= 0 {
			elapsed = 1
		}
		var totalLoss uint64
		peers = m.snapshotPeers(peers[:0])
		for _, p := range peers {
			remove, loss := p.update(elapsed)
			if remove {
				toRemove = append(toRemove, p)
			} else {
				totalLoss += loss
			}
		}
		clear(peers)
		if len(toRemove) > 0 {
			m.peersMu.Lock()
			for _, p := range toRemove {
				m.removePeerLocked(p)
			}
			m.peersMu.Unlock()
			clear(toRemove)
			toRemove = toRemove[:0]
		}
		m.packetLoss.Store(totalLoss)

		sleep := m.UpdateTime - (int(time.Since(last).Milliseconds()) - elapsed)
		last = time.Now()
		if sleep > 0 {
			timer.Reset(time.Duration(sleep) * time.Millisecond)
			select {
			case <-m.stop:
				return
			case <-timer.C:
			}
		}
	}
}

func (m *Manager) receiveLoop(conn *net.UDPConn) {
	defer m.wg.Done()
	// One spare byte detects datagrams above the reference receive buffer size.
	buf := make([]byte, maxPacketSize+1)
	for {
		n, addr, err := conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if errors.Is(err, errConnectionReset) || errors.Is(err, errMessageSize) || errors.Is(err, errTimedOut) {
				continue // ignored, as in NetSocket.ReceiveLogic
			}
			writeError("[R]Error code: %v", err)
			m.pushEvent(m.newEvent(evError, nil, netip.AddrPort{}, err))
			writeError("[NM] Receive error: %v", err)
			continue
		}
		if n > maxPacketSize {
			continue // the reference drops oversized datagrams (MessageSize)
		}
		if m.simulate(buf[:n], addr) {
			continue
		}
		m.dataReceived(buf[:n], addr)
	}
}

func (m *Manager) dataReceived(data []byte, addr netip.AddrPort) {
	defer func() {
		if e := recover(); e != nil { // protects the receive goroutine, as in the reference
			writeError("[NM] SocketReceiveThread error: %v", e)
		}
	}()
	m.packetsReceived.Add(1)
	m.bytesReceived.Add(uint64(len(data)))

	pk := getPacket(len(data), false)
	if !pk.fromBytes(data) {
		recyclePacket(pk)
		writeError("[NM] DataReceived: bad!")
		return
	}
	peer := m.tryGetPeer(addr)

	switch pk.property() {
	case propPeerNotFound:
		if peer != nil {
			if peer.ConnectionState() != StateConnected {
				return
			}
			if len(pk.data) == 1 {
				// first reply: send our connection id
				reply := getWithProperty(propPeerNotFound, 9)
				reply.data[1] = 0
				binary.LittleEndian.PutUint64(reply.data[2:], uint64(peer.currentConnectTime()))
				m.sendAndRecycle(reply, addr)
			} else if len(pk.data) == 10 && pk.data[1] == 1 &&
				int64(binary.LittleEndian.Uint64(pk.data[2:])) == peer.currentConnectTime() {
				// second reply: the remote really lost us
				m.disconnectPeer(peer, RemoteConnectionClose, nil, true, nil, nil)
			}
		} else if len(pk.data) == 10 && pk.data[1] == 0 {
			pk.data[1] = 1
			m.sendAndRecycle(pk, addr)
		}

	case propDiscoveryRequest:
		if m.DiscoveryEnabled {
			m.pushUnconnected(evDiscoveryRequest, addr, pk)
		}
	case propDiscoveryResponse:
		m.pushUnconnected(evDiscoveryResponse, addr, pk)
	case propUnconnectedMessage:
		if m.UnconnectedMessagesEnabled {
			m.pushUnconnected(evReceiveUnconnected, addr, pk)
		}
	case propNatIntroduction, propNatIntroductionRequest, propNatPunchMessage:
		if m.NatPunchEnabled {
			m.NatPunchModule.processMessage(addr, pk)
		}
	case propInvalidProtocol, propConnectAccept:
		// only meaningful for outgoing (client) connections

	case propDisconnect:
		if peer == nil {
			recyclePacket(pk)
		} else if !peer.remoteDisconnect(pk) {
			recyclePacket(pk)
			return // not ours: no ShutdownOk
		}
		m.sendAndRecycle(getWithProperty(propShutdownOk, 0), addr)

	case propConnectRequest:
		if int32(binary.LittleEndian.Uint32(pk.data[1:])) != protocolID {
			m.sendAndRecycle(getWithProperty(propInvalidProtocol, 0), addr)
			return
		}
		m.processConnectRequest(addr, peer, pk)

	default:
		if peer != nil {
			peer.processPacket(pk)
		} else {
			m.sendAndRecycle(getWithProperty(propPeerNotFound, 0), addr)
		}
	}
}

func (m *Manager) processConnectRequest(addr netip.AddrPort, peer *Peer, pk *packet) {
	connectNum := pk.connectionNumber()
	if connectNum >= MaxConnectionNumber {
		return
	}
	connectTime := int64(binary.LittleEndian.Uint64(pk.data[5:]))
	data := &DataReader{}
	if len(pk.data) > connectRequestHeaderSize {
		data.setSource(pk.data, connectRequestHeaderSize, len(pk.data))
	}

	if peer != nil {
		result, current := peer.processConnectRequest(connectTime, connectNum)
		switch result {
		case connectReconnection:
			m.disconnectPeer(peer, RemoteConnectionClose, nil, true, nil, nil)
			m.removePeer(peer)
		case connectNewConnection:
			m.removePeer(peer)
		default:
			return
		}
		connectNum = (current + 1) % MaxConnectionNumber
	}

	peer = newPeer(m, addr, m.nextPeerID())
	if m.tryAddPeer(peer) == peer {
		evt := m.newEvent(evConnectionRequest, nil, netip.AddrPort{}, nil)
		evt.request = &ConnectionRequest{Data: data, connectionID: connectTime, connectNum: connectNum, peer: peer}
		m.pushEvent(evt)
	}
}

func (m *Manager) onConnectionSolved(r *ConnectionRequest, rejectData []byte) {
	if r.Result() == RequestReject {
		r.peer.reject(r.connectionID, r.connectNum, rejectData)
		return
	}
	r.peer.accept(r.connectionID, r.connectNum)
	m.connectedPeers.Add(1)
	m.pushEvent(m.newEvent(evConnect, r.peer, netip.AddrPort{}, nil))
}

func (m *Manager) receiveFromPeer(p *Peer, pk *packet) {
	var method DeliveryMethod
	switch pk.property() {
	case propReliableUnordered:
		method = ReliableUnordered
	case propReliableOrdered:
		method = ReliableOrdered
	case propSequenced:
		method = Sequenced
	case propReliableSequenced:
		method = ReliableSequenced
	default:
		method = Unreliable
	}
	evt := m.newEvent(evReceive, p, p.addr, nil)
	evt.method = method
	evt.setSource(pk)
	m.pushEvent(evt)
}

func (m *Manager) connectionLatencyUpdated(p *Peer, latency int) {
	evt := m.newEvent(evLatencyUpdated, p, netip.AddrPort{}, nil)
	evt.latency = latency
	m.pushEvent(evt)
}

func (m *Manager) pushUnconnected(typ eventType, addr netip.AddrPort, pk *packet) {
	evt := m.newEvent(typ, nil, addr, nil)
	evt.setSource(pk)
	m.pushEvent(evt)
}
