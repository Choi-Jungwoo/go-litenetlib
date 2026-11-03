package litenetlib

import "net/netip"

// UnconnectedMessageType is the kind of message passed to OnNetworkReceiveUnconnected.
type UnconnectedMessageType byte

const (
	BasicMessage UnconnectedMessageType = iota
	DiscoveryRequest
	DiscoveryResponse
)

// DisconnectReason tells why a peer disconnected (INetEventListener.cs).
type DisconnectReason byte

const (
	ConnectionFailed DisconnectReason = iota
	Timeout
	HostUnreachable
	RemoteConnectionClose
	DisconnectPeerCalled
	ConnectionRejected
	InvalidProtocol
)

// DisconnectInfo carries details about a disconnection.
type DisconnectInfo struct {
	Reason DisconnectReason
	// SocketError is set when Reason is HostUnreachable or caused by a socket error.
	SocketError error
	// AdditionalData holds data sent with the remote disconnect (RemoteConnectionClose).
	AdditionalData *PacketReader
}

// EventListener receives network events, from PollEvents or, with UnsyncedEvents, from a library goroutine.
type EventListener interface {
	OnPeerConnected(peer *Peer)
	OnPeerDisconnected(peer *Peer, info DisconnectInfo)
	OnNetworkError(addr netip.AddrPort, err error)
	OnNetworkReceive(peer *Peer, reader *PacketReader, method DeliveryMethod)
	OnNetworkReceiveUnconnected(addr netip.AddrPort, reader *PacketReader, typ UnconnectedMessageType)
	OnNetworkLatencyUpdate(peer *Peer, latency int)
	OnConnectionRequest(request *ConnectionRequest)
}

// EventBasedListener adapts optional callbacks to EventListener; nil callbacks are ignored.
type EventBasedListener struct {
	PeerConnected             func(peer *Peer)
	PeerDisconnected          func(peer *Peer, info DisconnectInfo)
	NetworkError              func(addr netip.AddrPort, err error)
	NetworkReceive            func(peer *Peer, reader *PacketReader, method DeliveryMethod)
	NetworkReceiveUnconnected func(addr netip.AddrPort, reader *PacketReader, typ UnconnectedMessageType)
	NetworkLatencyUpdate      func(peer *Peer, latency int)
	ConnectionRequest         func(request *ConnectionRequest)
}

func (l *EventBasedListener) OnPeerConnected(peer *Peer) {
	if l.PeerConnected != nil {
		l.PeerConnected(peer)
	}
}

func (l *EventBasedListener) OnPeerDisconnected(peer *Peer, info DisconnectInfo) {
	if l.PeerDisconnected != nil {
		l.PeerDisconnected(peer, info)
	}
}

func (l *EventBasedListener) OnNetworkError(addr netip.AddrPort, err error) {
	if l.NetworkError != nil {
		l.NetworkError(addr, err)
	}
}

func (l *EventBasedListener) OnNetworkReceive(peer *Peer, reader *PacketReader, method DeliveryMethod) {
	if l.NetworkReceive != nil {
		l.NetworkReceive(peer, reader, method)
	}
}

func (l *EventBasedListener) OnNetworkReceiveUnconnected(addr netip.AddrPort, reader *PacketReader, typ UnconnectedMessageType) {
	if l.NetworkReceiveUnconnected != nil {
		l.NetworkReceiveUnconnected(addr, reader, typ)
	}
}

func (l *EventBasedListener) OnNetworkLatencyUpdate(peer *Peer, latency int) {
	if l.NetworkLatencyUpdate != nil {
		l.NetworkLatencyUpdate(peer, latency)
	}
}

func (l *EventBasedListener) OnConnectionRequest(request *ConnectionRequest) {
	if l.ConnectionRequest != nil {
		l.ConnectionRequest(request)
	}
}
