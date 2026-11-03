# go-litenetlib

A Go port of the [LiteNetLib](https://github.com/RevenantX/LiteNetLib) v0.8.3 server, the reliable UDP library for .NET. It matches that version byte for byte on the wire and behaves the same, so unmodified v0.8.3 .NET clients can connect to it.

Target version: [LiteNetLib v0.8.3](https://github.com/RevenantX/LiteNetLib/tree/v0.8.3) (protocol id 7). Other versions are not compatible.

## Scope

- **Server**: everything a v0.8.3 `NetManager` does for incoming connections: connection requests (accept, key check, reject with data), the connection state machine, reconnection, the reliable, ordered, sequenced and unreliable channels, fragmentation, packet merging, ping/RTT, MTU discovery, timeouts, graceful disconnects, PeerNotFound handling, unconnected messages, discovery responses and NAT punching (master server and host roles).
- **Utils**: `DataReader`/`DataWriter` (NetDataReader/NetDataWriter format), `PacketProcessor` (NetPacketProcessor/NetSerializer format), the NetUtils helpers (`ResolveAddress`, `MakeEndPoint`, `GetLocalIP`, `GetLocalIPList`) and the NetDebug logger (`SetLogger`).
- **Client**: not planned (`Connect`, discovery broadcast). Use the .NET LiteNetLib client.

## Installation

```sh
go get github.com/choi-jungwoo/go-litenetlib
```

## Usage

```go
listener := &litenetlib.EventBasedListener{
	ConnectionRequest: func(r *litenetlib.ConnectionRequest) { r.AcceptIfKey("my-key") },
	PeerConnected:     func(p *litenetlib.Peer) { log.Println("connected", p.EndPoint()) },
	NetworkReceive: func(p *litenetlib.Peer, r *litenetlib.PacketReader, m litenetlib.DeliveryMethod) {
		p.Send(r.GetRemainingBytes(), m) // echo
		r.Recycle()
	},
}
server := litenetlib.NewManager(listener)
if err := server.Start(9050); err != nil {
	log.Fatal(err)
}
defer server.Stop(true)
for {
	server.PollEvents()
	time.Sleep(15 * time.Millisecond)
}
```

Set `UnsyncedEvents` before `Start` to receive events on a library goroutine instead of calling `PollEvents`.

## Differences from the .NET API

- Errors are returned instead of thrown (`Start`, `Send` with `ErrTooBigPacket`, `PacketProcessor`).
- Addresses are `netip.AddrPort`. Overloads taking `start`/`length` are replaced by slices.
- `PacketProcessor` identifies a packet type by the full name of its C# counterpart, passed explicitly (`"MyGame.JoinPacket"`). Exported struct fields map to the C# public properties in declaration order.
- Library errors go to the standard `log` package unless `SetLogger` installs a `NetLogger` (the reference writes them to the console).

## Release and debug builds

Like the reference, the default build matches a Release build of LiteNetLib: most statistics are not collected and `SimulatePacketLoss`/`SimulateLatency` have no effect. Build with `-tags debug` to get the reference DEBUG behavior (`STATS_ENABLED` statistics and the network simulation).

## Development

```sh
mise run test   # go test -race, release and debug builds
mise run bench  # hot path benchmarks
```

The interop tests build a .NET client from the unmodified v0.8.3 sources and run it against the Go server. They expect the reference checkout at `~/Code/LiteNetLib` (override with `LITENETLIB_SRC=/path/to/LiteNetLib/LiteNetLib`) and are skipped when `dotnet` is not installed. CI (`.github/workflows/ci.yml`) runs everything on Linux, macOS and Windows.

## License

[MIT](LICENSE)
