// Interop client: drives the unmodified LiteNetLib v0.8.3 client against the Go server.
// Usage: client <scenario> <serverPort>. Prints "PASS" and exits 0 on success.
using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using LiteNetLib;
using LiteNetLib.Utils;

static class Program
{
    static int Port;
    static bool Lossy; // traffic goes through a lossy, reordering proxy

    static int Main(string[] args)
    {
        Port = int.Parse(args[1]);
        Lossy = args.Length > 2 && args[2] == "lossy";
        try
        {
            switch (args[0])
            {
                case "connect": Connect(); break;
                case "client_disconnect": ClientDisconnect(); break;
                case "server_disconnect": ServerDisconnect(); break;
                case "reject": Reject(); break;
                case "wrong_key": WrongKey(); break;
                case "echo": Echo(); break;
                case "mtu_ping": MtuPing(); break;
                case "vanish": Vanish(); break;
                case "unconnected": Unconnected(); break;
                case "serialize": Serialize(); break;
                case "reconnect": Reconnect(); break;
                case "nat": Nat(); break;
                case "packet_processor": PacketProcessorScenario(); break;
                default: throw new Exception("unknown scenario " + args[0]);
            }
            Console.WriteLine("PASS");
            return 0;
        }
        catch (Exception e)
        {
            Console.WriteLine("FAIL: " + e.Message);
            return 1;
        }
    }

    static void Check(bool cond, string what)
    {
        if (!cond) throw new Exception(what);
    }

    static void Until(NetManager m, Func<bool> cond, string what, int timeoutMs = 3000)
    {
        var sw = Stopwatch.StartNew();
        while (!cond())
        {
            m.PollEvents();
            if (sw.ElapsedMilliseconds > timeoutMs) throw new Exception("timeout waiting for " + what);
            Thread.Sleep(5);
        }
    }

    static void Hold(NetManager m, int ms)
    {
        var sw = Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < ms) { m.PollEvents(); Thread.Sleep(5); }
    }

    static NetManager Start(EventBasedNetListener l)
    {
        var m = new NetManager(l);
        Check(m.Start(), "client start");
        return m;
    }

    static NetPeer ConnectPeer(NetManager m, EventBasedNetListener l, string key = "key")
    {
        NetPeer connected = null;
        l.PeerConnectedEvent += p => connected = p;
        var peer = m.Connect(new IPEndPoint(IPAddress.Loopback, Port), key);
        Until(m, () => connected != null, "connected", Lossy ? 10000 : 3000);
        Check(connected == peer, "connected peer mismatch");
        return peer;
    }

    static void Connect()
    {
        var l = new EventBasedNetListener();
        var m = Start(l);
        var peer = ConnectPeer(m, l);
        Check(peer.ConnectionState == ConnectionState.Connected, "state connected");
        m.Stop();
    }

    static void ClientDisconnect()
    {
        var l = new EventBasedNetListener();
        var m = Start(l);
        var peer = ConnectPeer(m, l);
        peer.Disconnect(new byte[] { 1, 2, 3 });
        // Disconnected (not ShutdownRequested) means the server answered with ShutdownOk.
        Until(m, () => peer.ConnectionState == ConnectionState.Disconnected, "ShutdownOk");
        m.Stop();
    }
    static void ServerDisconnect()
    {
        var l = new EventBasedNetListener();
        var m = Start(l);
        DisconnectInfo? info = null;
        byte[] data = null;
        l.PeerDisconnectedEvent += (p, i) => { info = i; data = i.AdditionalData.GetRemainingBytes(); };
        ConnectPeer(m, l);
        Until(m, () => info != null, "disconnect");
        Check(info.Value.Reason == DisconnectReason.RemoteConnectionClose, "reason " + info.Value.Reason);
        Check(data.Length == 3 && data[0] == 9 && data[1] == 8 && data[2] == 7, "disconnect data " + BitConverter.ToString(data));
        Thread.Sleep(100); // let ShutdownOk reach the server
        m.Stop();
    }

    static void Reject()
    {
        var l = new EventBasedNetListener();
        var m = Start(l);
        DisconnectInfo? info = null;
        byte[] data = null;
        l.PeerDisconnectedEvent += (p, i) => { info = i; data = i.AdditionalData.GetRemainingBytes(); };
        m.Connect(new IPEndPoint(IPAddress.Loopback, Port), "reject-me");
        Until(m, () => info != null, "reject");
        Check(info.Value.Reason == DisconnectReason.ConnectionRejected, "reason " + info.Value.Reason);
        Check(data.Length == 2 && data[0] == 5 && data[1] == 5, "reject data " + BitConverter.ToString(data));
        m.Stop();
    }

    static void WrongKey()
    {
        var l = new EventBasedNetListener();
        var m = Start(l);
        DisconnectInfo? info = null;
        l.PeerDisconnectedEvent += (p, i) => info = i;
        m.Connect(new IPEndPoint(IPAddress.Loopback, Port), "wrong");
        Until(m, () => info != null, "reject");
        Check(info.Value.Reason == DisconnectReason.ConnectionRejected, "reason " + info.Value.Reason);
        m.Stop();
    }

    static void Echo() { RunEcho(Lossy); }

    // Message: [method][int seq][pattern bytes derived from seq].
    static byte[] Message(DeliveryMethod method, int seq, int length)
    {
        var w = new NetDataWriter();
        w.Put((byte)method);
        w.Put(seq);
        for (int i = 0; i < length; i++) w.Put((byte)(seq * 31 + i));
        return w.CopyData();
    }

    static int Verify(NetPacketReader r, DeliveryMethod method)
    {
        Check(r.GetByte() == (byte)method, "method byte for " + method);
        int seq = r.GetInt();
        for (int i = 0; r.AvailableBytes > 0; i++)
            Check(r.GetByte() == (byte)(seq * 31 + i), "payload of " + method + " #" + seq);
        return seq;
    }

    static void RunEcho(bool loss)
    {
        var l = new EventBasedNetListener();
        var got = new System.Collections.Generic.Dictionary<DeliveryMethod, System.Collections.Generic.List<int>>();
        foreach (DeliveryMethod dm in Enum.GetValues(typeof(DeliveryMethod))) got[dm] = new System.Collections.Generic.List<int>();
        l.NetworkReceiveEvent += (p, r, dm) => { got[dm].Add(Verify(r, dm)); r.Recycle(); };
        var m = Start(l);
        var peer = ConnectPeer(m, l);

        const int n = 200;
        foreach (DeliveryMethod dm in Enum.GetValues(typeof(DeliveryMethod)))
            for (int i = 0; i < n; i++)
                peer.Send(Message(dm, i, (i * 7) % 300), dm);
        // Fragmented payloads (well above the 508-byte starting MTU).
        peer.Send(Message(DeliveryMethod.ReliableOrdered, n, 20000), DeliveryMethod.ReliableOrdered);
        peer.Send(Message(DeliveryMethod.ReliableUnordered, n, 5000), DeliveryMethod.ReliableUnordered);

        Until(m, () => got[DeliveryMethod.ReliableOrdered].Count == n + 1 && got[DeliveryMethod.ReliableUnordered].Count == n + 1,
            "reliable echoes", 20000);
        var ordered = got[DeliveryMethod.ReliableOrdered];
        for (int i = 0; i <= n; i++) Check(ordered[i] == i, "ordered position " + i + " got " + ordered[i]);
        var unordered = new System.Collections.Generic.HashSet<int>(got[DeliveryMethod.ReliableUnordered]);
        Check(unordered.Count == n + 1, "unordered unique count " + unordered.Count);
        Until(m, () => got[DeliveryMethod.ReliableSequenced].Count > 0 && got[DeliveryMethod.ReliableSequenced][got[DeliveryMethod.ReliableSequenced].Count - 1] == n - 1,
            "last reliable sequenced", 20000);
        foreach (var dm in new[] { DeliveryMethod.Sequenced, DeliveryMethod.ReliableSequenced })
        {
            var s = got[dm];
            for (int i = 1; i < s.Count; i++) Check(s[i] > s[i - 1], dm + " not increasing at " + i);
        }
        if (!loss)
            Check(got[DeliveryMethod.Unreliable].Count == n && got[DeliveryMethod.Sequenced].Count == n, "unreliable echoes on loopback");
        m.Stop();
    }

    static void MtuPing()
    {
        var l = new EventBasedNetListener();
        int latencyUpdates = 0;
        l.NetworkLatencyUpdateEvent += (p, latency) => latencyUpdates++;
        var m = Start(l);
        var peer = ConnectPeer(m, l);
        Until(m, () => peer.Mtu == 1432, "client MTU 1432 (now " + peer.Mtu + ")", 9000);
        Until(m, () => latencyUpdates >= 2, "latency updates from server pongs", 3000);
        Check(Math.Abs(peer.RemoteTimeDelta) < TimeSpan.TicksPerSecond, "remote time delta " + peer.RemoteTimeDelta);
        Hold(m, 1500); // stay connected while the server finishes probing
        m.Stop();
    }

    static void Vanish()
    {
        var l = new EventBasedNetListener();
        var m = Start(l);
        ConnectPeer(m, l);
        m.Stop(false); // no disconnect message
    }

    static void Unconnected()
    {
        var l = new EventBasedNetListener();
        string reply = null, discovery = null;
        l.NetworkReceiveUnconnectedEvent += (ep, r, type) =>
        {
            if (type == UnconnectedMessageType.BasicMessage) reply = r.GetString();
            if (type == UnconnectedMessageType.DiscoveryResponse) discovery = r.GetString();
        };
        var m = new NetManager(l) { UnconnectedMessagesEnabled = true };
        Check(m.Start(), "client start");
        Check(m.SendUnconnectedMessage(NetDataWriter.FromString("ping"), new IPEndPoint(IPAddress.Loopback, Port)), "send unconnected");
        Until(m, () => reply != null, "unconnected reply");
        Check(reply == "pong", "reply " + reply);
        // SendDiscoveryRequest's IPv6 multicast throws on macOS, so ask with an unconnected message.
        Check(m.SendUnconnectedMessage(NetDataWriter.FromString("who"), new IPEndPoint(IPAddress.Loopback, Port)), "send who");
        Until(m, () => discovery != null, "discovery response");
        Check(discovery == "me", "discovery " + discovery);
        m.Stop();
    }

    static void Serialize()
    {
        var l = new EventBasedNetListener();
        bool verified = false;
        l.NetworkReceiveEvent += (p, r, dm) =>
        {
            Check(r.GetByte() == 200, "byte");
            Check(r.GetSByte() == -5, "sbyte");
            Check(r.GetBool(), "bool");
            Check(r.GetChar() == 'Ж', "char");
            Check(r.GetUShort() == 65000, "ushort");
            Check(r.GetShort() == -1234, "short");
            Check(r.GetInt() == -123456789, "int");
            Check(r.GetUInt() == 4000000000u, "uint");
            Check(r.GetLong() == -1234567890123L, "long");
            Check(r.GetULong() == 18000000000000000000ul, "ulong");
            Check(r.GetFloat() == 3.25f, "float");
            Check(r.GetDouble() == -1.5e100, "double");
            Check(r.GetString() == "héllo wörld 🌍", "string");
            Check(r.GetString(10) == "héllo", "string max");
            Check(r.GetString() == "", "empty string");
            var b = r.GetBytesWithLength();
            Check(b.Length == 3 && b[2] == 3, "bytes with length");
            var ia = r.GetIntArray();
            Check(ia.Length == 3 && ia[1] == -2, "int array");
            var sa = r.GetStringArray();
            Check(sa.Length == 2 && sa[1] == "bc", "string array");
            var ba = r.GetBoolArray();
            Check(ba.Length == 2 && ba[0] && !ba[1], "bool array");
            var fa = r.GetFloatArray();
            Check(fa.Length == 1 && fa[0] == 1.5f, "float array");
            var ep = r.GetNetEndPoint();
            Check(ep.ToString() == "10.1.2.3:4567", "endpoint " + ep);
            Check(r.EndOfData, "end of data");
            verified = true;
        };
        var m = Start(l);
        var peer = ConnectPeer(m, l);
        var w = new NetDataWriter();
        w.Put((byte)200); w.Put((sbyte)-5); w.Put(true); w.Put('Ж'); w.Put((ushort)65000); w.Put((short)-1234);
        w.Put(-123456789); w.Put(4000000000u); w.Put(-1234567890123L); w.Put(18000000000000000000ul);
        w.Put(3.25f); w.Put(-1.5e100); w.Put("héllo wörld 🌍"); w.Put("héllo", 10); w.Put("");
        w.PutBytesWithLength(new byte[] { 1, 2, 3 }); w.PutArray(new[] { 1, -2, 3 }); w.PutArray(new[] { "a", "bc" });
        w.PutArray(new[] { true, false }); w.PutArray(new[] { 1.5f }); w.Put(new IPEndPoint(IPAddress.Parse("10.1.2.3"), 4567));
        peer.Send(w, DeliveryMethod.ReliableOrdered);
        Until(m, () => verified, "server echo of serialized values");
        m.Stop();
    }

    static void EchoOnce(NetManager m, EventBasedNetListener l, NetPeer peer, string text)
    {
        string got = null;
        EventBasedNetListener.OnNetworkReceive h = (p, r, dm) => got = r.GetString();
        l.NetworkReceiveEvent += h;
        var w = new NetDataWriter();
        w.Put(text);
        peer.Send(w, DeliveryMethod.ReliableOrdered);
        Until(m, () => got != null, "echo " + text);
        Check(got == text, "echo " + text + " got " + got);
        l.NetworkReceiveEvent -= h;
    }

    static void Reconnect()
    {
        var l1 = new EventBasedNetListener();
        var m1 = Start(l1);
        int localPort = m1.LocalPort;
        EchoOnce(m1, l1, ConnectPeer(m1, l1), "first");
        m1.Stop(false); // vanish; the next request from the same endpoint is a reconnection

        var l2 = new EventBasedNetListener();
        var m2 = new NetManager(l2);
        Check(m2.Start(localPort), "restart on same port");
        var peer = ConnectPeer(m2, l2);
        EchoOnce(m2, l2, peer, "second");

        peer.Disconnect();
        Until(m2, () => peer.ConnectionState == ConnectionState.Disconnected, "ShutdownOk");
        NetPeer third = null;
        l2.PeerConnectedEvent += p => third = p;
        m2.Connect(new IPEndPoint(IPAddress.Loopback, Port), "key");
        Until(m2, () => third != null, "third connection");
        EchoOnce(m2, l2, third, "third");
        m2.Stop();
    }

    // Two v0.8.3 peers get introduced by the Go master server, punch, then connect.
    static void Nat()
    {
        var master = new IPEndPoint(IPAddress.Loopback, Port);
        var hostListener = new EventBasedNetListener();
        hostListener.ConnectionRequestEvent += r => r.AcceptIfKey("nat");
        var host = new NetManager(hostListener) { NatPunchEnabled = true };
        Check(host.Start(), "host start");
        var clientListener = new EventBasedNetListener();
        var client = new NetManager(clientListener) { NatPunchEnabled = true };
        Check(client.Start(), "client start");

        IPEndPoint hostSeen = null, clientSeen = null;
        var hostNat = new EventBasedNatPunchListener();
        hostNat.NatIntroductionSuccess += (ep, token) => { if (token == "tok") clientSeen = ep; };
        host.NatPunchModule.Init(hostNat);
        var clientNat = new EventBasedNatPunchListener();
        clientNat.NatIntroductionSuccess += (ep, token) => { if (token == "tok") hostSeen = ep; };
        client.NatPunchModule.Init(clientNat);

        host.NatPunchModule.SendNatIntroduceRequest(master, "tok");
        Thread.Sleep(100); // the master introduces in arrival order: host first
        client.NatPunchModule.SendNatIntroduceRequest(master, "tok");
        Until(client, () =>
        {
            host.NatPunchModule.PollEvents();
            client.NatPunchModule.PollEvents();
            host.PollEvents();
            return hostSeen != null && clientSeen != null;
        }, "punch success on both sides");
        Check(hostSeen.Port == host.LocalPort, "client punched by host port " + hostSeen.Port);
        Check(clientSeen.Port == client.LocalPort, "host punched by client port " + clientSeen.Port);

        NetPeer connected = null;
        clientListener.PeerConnectedEvent += p => connected = p;
        client.Connect(hostSeen, "nat");
        Until(client, () => { host.PollEvents(); return connected != null; }, "connect through punched hole");
        host.Stop();
        client.Stop();
    }

    static Interop.SamplePacket Sample()
    {
        return new Interop.SamplePacket
        {
            Id = 77, Name = "naïve 🌍", Color = Interop.Color.Blue, Flag = true, Blob = new byte[] { 9, 8 },
            Numbers = new[] { 1, -2 }, Tags = new[] { "x", "yz" }, Pos = new Interop.Vec { X = 1.5f, Y = -2 },
            Path = new[] { new Interop.Vec { X = 3, Y = 4 } }, Big = -5000000000L, D = 0.25, C = 'Ж'
        };
    }

    static void PacketProcessorScenario()
    {
        var l = new EventBasedNetListener();
        var pp = new NetPacketProcessor();
        pp.RegisterNestedType<Interop.Vec>();
        Interop.SamplePacket echoed = null;
        Interop.Ping ping = null;
        pp.Subscribe<Interop.SamplePacket>(p => echoed = p, () => new Interop.SamplePacket());
        pp.SubscribeNetSerializable<Interop.Ping>(p => ping = p, () => new Interop.Ping());
        l.NetworkReceiveEvent += (p, r, dm) => { pp.ReadAllPackets(r); r.Recycle(); };
        var m = Start(l);
        var peer = ConnectPeer(m, l);
        var w = new NetDataWriter();
        pp.Write(w, Sample());
        pp.WriteNetSerializable(w, new Interop.Ping { Seq = 5 });
        peer.Send(w, DeliveryMethod.ReliableOrdered);
        Until(m, () => echoed != null && ping != null, "processor echo");
        var s = Sample();
        Check(echoed.Id == s.Id && echoed.Name == s.Name && echoed.Color == s.Color && echoed.Flag, "scalars");
        Check(echoed.Blob.Length == 2 && echoed.Blob[1] == 8, "blob");
        Check(echoed.Numbers[1] == -2 && echoed.Tags[1] == "yz", "arrays");
        Check(echoed.Pos.X == 1.5f && echoed.Pos.Y == -2 && echoed.Path[0].Y == 4, "nested");
        Check(echoed.Big == s.Big && echoed.D == s.D && echoed.C == s.C, "wide scalars");
        Check(ping.Seq == 6, "net serializable");
        m.Stop();
    }

    // scenarios end
}

namespace Interop
{
    public enum Color : byte { Red = 1, Blue = 2 }

    public struct Vec : INetSerializable
    {
        public float X, Y;
        public void Serialize(NetDataWriter w) { w.Put(X); w.Put(Y); }
        public void Deserialize(NetDataReader r) { X = r.GetFloat(); Y = r.GetFloat(); }
    }

    public class SamplePacket
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public Color Color { get; set; }
        public bool Flag { get; set; }
        public byte[] Blob { get; set; }
        public int[] Numbers { get; set; }
        public string[] Tags { get; set; }
        public Vec Pos { get; set; }
        public Vec[] Path { get; set; }
        public long Big { get; set; }
        public double D { get; set; }
        public char C { get; set; }
    }

    public class Ping : INetSerializable
    {
        public int Seq;
        public void Serialize(NetDataWriter w) { w.Put(Seq); }
        public void Deserialize(NetDataReader r) { Seq = r.GetInt(); }
    }
}
