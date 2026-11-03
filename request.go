package litenetlib

import (
	"net/netip"
	"sync/atomic"
)

// ConnectionRequestResult is the outcome of a ConnectionRequest.
type ConnectionRequestResult byte

const (
	RequestNone ConnectionRequestResult = iota
	RequestAccept
	RequestReject
)

// ConnectionRequest is an incoming connection waiting for Accept or Reject (ConnectionRequest.cs).
type ConnectionRequest struct {
	// Data is the connection data sent by the client.
	Data *DataReader

	used         atomic.Bool
	result       atomic.Uint32
	connectionID int64
	connectNum   byte
	peer         *Peer
}

// RemoteEndPoint is the address of the connecting client.
func (r *ConnectionRequest) RemoteEndPoint() netip.AddrPort { return r.peer.addr }

// Result reports whether the request was accepted or rejected.
func (r *ConnectionRequest) Result() ConnectionRequestResult {
	return ConnectionRequestResult(r.result.Load())
}

func (r *ConnectionRequest) tryActivate() bool { return r.used.CompareAndSwap(false, true) }

// AcceptIfKey accepts the connection if the data is a string equal to key, otherwise rejects it.
func (r *ConnectionRequest) AcceptIfKey(key string) *Peer {
	if !r.tryActivate() {
		return nil
	}
	if s, ok := r.Data.TryGetString(); ok && s == key {
		r.solve(RequestAccept, nil)
		return r.peer
	}
	r.solve(RequestReject, nil)
	return nil
}

// Accept accepts the connection and returns the connected peer, or nil if already solved.
func (r *ConnectionRequest) Accept() *Peer {
	if !r.tryActivate() {
		return nil
	}
	r.solve(RequestAccept, nil)
	return r.peer
}

// Reject rejects the connection, sending rejectData (may be nil) to the client.
func (r *ConnectionRequest) Reject(rejectData []byte) {
	if !r.tryActivate() {
		return
	}
	r.solve(RequestReject, rejectData)
}

func (r *ConnectionRequest) solve(result ConnectionRequestResult, rejectData []byte) {
	r.result.Store(uint32(result))
	r.peer.m.onConnectionSolved(r, rejectData)
}
