using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

public class Program
{

    public static async Task Main(string[] args)
    {

        switch(args[0])
        {
            case "server":
                await Program1.RunAsync(new[] { args[1] });
                break;
            case "client":
                await Program2.RunAsync(new[] { args[1], args[2], args[3] });
                break;
            default: break;
        }

    }

}

sealed class ClientState
{
    public TcpClient Client { get; }
    public StreamReader Reader { get; }
    public StreamWriter Writer { get; }
    public string Username { get; set; } = "";
    public IPAddress RemoteIp { get; }

    public ClientState(TcpClient c)
    {
        Client = c;
        var ns = c.GetStream();
        Reader = new StreamReader(ns, Encoding.UTF8, leaveOpen: true);
        Writer = new StreamWriter(ns, Encoding.UTF8) { AutoFlush = true };
        RemoteIp = ((IPEndPoint)c.Client.RemoteEndPoint!).Address;
    }
}

class Program1
{
    static readonly ConcurrentDictionary<string, ClientState> Users =
        new ConcurrentDictionary<string, ClientState>(StringComparer.OrdinalIgnoreCase);

    public static async Task RunAsync(string[] args)
    {
        if (args.Length < 1)
        {
            Console.WriteLine("Usage: Server <port>");
            return;
        }

        int port = int.Parse(args[0]);
        var listener = new TcpListener(IPAddress.Any, port);
        listener.Start();
        Console.WriteLine($"Chat-Server gestartet auf Port {port}");

        while (true)
        {
            var tcp = await listener.AcceptTcpClientAsync();
            _ = Task.Run(() => HandleClientAsync(tcp));
        }
    }

    static async Task HandleClientAsync(TcpClient tcp)
    {
        var cs = new ClientState(tcp);

        try
        {
            await cs.Writer.WriteLineAsync("Waehle einen Username:");
            var name = await cs.Reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(name)) return;

            cs.Username = name.Trim();
            if (!Users.TryAdd(cs.Username, cs))
            {
                await cs.Writer.WriteLineAsync("Username already taken.");
                return;
            }

            Console.WriteLine($"[INFO] User authenticated: {cs.Username} (IP {cs.RemoteIp})");

            while (true)
            {
                var line = await cs.Reader.ReadLineAsync();
                if (line == null) break;

                line = line.TrimEnd('\r', '\n');
                if (line.Length == 0) continue;

                if (line.StartsWith("#", StringComparison.Ordinal))
                    await HandleCommandAsync(cs, line);
                else
                    Console.WriteLine($"[SERVER] from {cs.Username}: {line}");
            }
        }
        catch (IOException) { }
        catch (ObjectDisposedException) { }
        finally
        {
            if (!string.IsNullOrEmpty(cs.Username))
                Users.TryRemove(cs.Username, out _);

            try { tcp.Close(); } catch { }
            Console.WriteLine($"[INFO] Client disconnected: {cs.Username}");
        }
    }

    static async Task HandleCommandAsync(ClientState from, string line)
    {
        var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var cmd = parts[0].Substring(1);

        if (cmd.Equals("invite", StringComparison.OrdinalIgnoreCase))
        {
            if (parts.Length != 3 || !int.TryParse(parts[2], out int p2pPort) || p2pPort <= 0 || p2pPort > 65535)
            {
                await from.Writer.WriteLineAsync("Usage: #invite <user> <port>");
                return;
            }

            var recipient = parts[1];
            if (!Users.TryGetValue(recipient, out var target))
            {
                await from.Writer.WriteLineAsync($"Recipient '{recipient}' not found");
                return;
            }

            var inviterIp = from.RemoteIp.ToString();
            await target.Writer.WriteLineAsync($"#p2p_offer {from.Username} {inviterIp} {p2pPort}");
            await from.Writer.WriteLineAsync($"[INVITE] sent to {recipient}");

            Console.WriteLine($"[INVITE] {from.Username} -> {recipient} : {inviterIp}:{p2pPort}");
            return;
        }

        if (cmd.Equals("userlist", StringComparison.OrdinalIgnoreCase))
        {
            var list = string.Join(",", Users.Keys.OrderBy(x => x));
            await from.Writer.WriteLineAsync(string.IsNullOrEmpty(list) ? "(no users)" : list);
            return;
        }

        await from.Writer.WriteLineAsync("Unknown command");
    }
}

sealed class PeerConn
{
    public TcpClient Client { get; }
    public StreamReader Reader { get; }
    public StreamWriter Writer { get; }
    public string Name { get; set; }

    public PeerConn(TcpClient c, string name)
    {
        Client = c;
        var ns = c.GetStream();
        Reader = new StreamReader(ns, Encoding.UTF8, leaveOpen: true);
        Writer = new StreamWriter(ns, Encoding.UTF8) { AutoFlush = true };
        Name = name;
    }

    public void Close()
    {
        try { Client.Client.Shutdown(SocketShutdown.Both); } catch { }
        try { Client.Close(); } catch { }
    }
}

sealed record Offer(string PeerName, string Ip, int Port, DateTime ReceivedAt);

class Program2
{
    static readonly ConcurrentDictionary<string, PeerConn> Peers =
        new ConcurrentDictionary<string, PeerConn>(StringComparer.OrdinalIgnoreCase);

    static readonly ConcurrentDictionary<string, Offer> Offers =
        new ConcurrentDictionary<string, Offer>(StringComparer.OrdinalIgnoreCase);

    static string MyName = "anon";

    public static async Task RunAsync(string[] args)
    {
        if (args.Length < 3)
        {
            Console.WriteLine("Usage: Client <server_ip> <server_port> <my_p2p_port>");
            return;
        }

        string serverIp = args[0];
        int serverPort = int.Parse(args[1]);
        int myP2pPort = int.Parse(args[2]);

        var server = new TcpClient();
        await server.ConnectAsync(IPAddress.Parse(serverIp), serverPort);

        var serverNs = server.GetStream();
        var serverReader = new StreamReader(serverNs, Encoding.UTF8, leaveOpen: true);
        var serverWriter = new StreamWriter(serverNs, Encoding.UTF8) { AutoFlush = true };

        Console.WriteLine("Verbunden mit dem Chat-Server");

        var prompt = await serverReader.ReadLineAsync();
        if (prompt != null) Console.WriteLine($"<SERVER> {prompt}");

        MyName = (Console.ReadLine() ?? "anon").Trim();
        if (MyName.Length == 0) MyName = "anon";
        await serverWriter.WriteLineAsync(MyName);

        // P2P listener (für incoming)
        var p2pListener = new TcpListener(IPAddress.Any, myP2pPort);
        p2pListener.Start();
        _ = Task.Run(() => AcceptPeersLoopAsync(p2pListener));

        // Server recv loop
        _ = Task.Run(() => ServerRecvLoopAsync(serverReader));

        Console.WriteLine("Commands:");
        Console.WriteLine("  #invite <user> <port>   (P2P Offer senden)");
        Console.WriteLine("  /offers                 (eingehende Offers anzeigen)");
        Console.WriteLine("  /accept <user>          (Offer annehmen -> P2P verbinden)");
        Console.WriteLine("  /deny <user>            (Offer ablehnen)");
        Console.WriteLine("  /p <peer> <text>        (P2P Nachricht)");
        Console.WriteLine("  /plist                  (aktive Peers)");
        Console.WriteLine("  /pclose <peer>          (Peer schließen)");
        Console.WriteLine("  /pcloseall              (alle schließen)");

        while (true)
        {
            var raw = Console.ReadLine();
            if (raw == null) break;

            var line = raw.Trim(); // wichtig: führende Spaces weg

            if (line.Equals("/plist", StringComparison.OrdinalIgnoreCase))
            {
                PrintPeers();
                continue;
            }

            if (line.Equals("/offers", StringComparison.OrdinalIgnoreCase))
            {
                PrintOffers();
                continue;
            }

            if (line.Equals("/pcloseall", StringComparison.OrdinalIgnoreCase))
            {
                CloseAllPeers();
                continue;
            }

            if (line.StartsWith("/pclose ", StringComparison.OrdinalIgnoreCase))
            {
                var peer = line.Substring(8).Trim();
                if (peer.Length > 0) ClosePeer(peer);
                continue;
            }

            if (line.StartsWith("/deny ", StringComparison.OrdinalIgnoreCase))
            {
                var peer = line.Substring(6).Trim();
                if (peer.Length > 0) DenyOffer(peer);
                continue;
            }

            if (line.StartsWith("/accept ", StringComparison.OrdinalIgnoreCase))
            {
                var peer = line.Substring(8).Trim();
                if (peer.Length > 0)
                {
                    if (Peers.ContainsKey(peer))
                    {
                        Console.WriteLine($"[P2P] already connected to {peer}");
                    }
                    else if (Offers.TryRemove(peer, out var offer))
                    {
                        _ = Task.Run(() => ConnectToPeerAsync(offer.PeerName, offer.Ip, offer.Port));
                    }
                    else
                    {
                        Console.WriteLine($"[P2P] no pending offer from '{peer}'");
                    }
                }
                continue;
            }

            if (line.StartsWith("/p ", StringComparison.OrdinalIgnoreCase))
            {
                var rest = line.Substring(3);
                var sp = rest.IndexOf(' ');
                if (sp <= 0)
                {
                    Console.WriteLine("[P2P] Usage: /p <peer> <text>");
                    continue;
                }
                var peer = rest.Substring(0, sp).Trim();
                var text = rest.Substring(sp + 1);
                await SendToPeer(peer, text);
                continue;
            }

            // default -> server
            await serverWriter.WriteLineAsync(line);
        }
    }

    static void PrintPeers()
    {
        var names = Peers.Keys.OrderBy(x => x).ToArray();
        Console.WriteLine(names.Length == 0 ? "[P2P] no active peers" : "[P2P] peers: " + string.Join(", ", names));
    }

    static void PrintOffers()
    {
        var offers = Offers.Values.OrderBy(o => o.ReceivedAt).ToArray();
        if (offers.Length == 0)
        {
            Console.WriteLine("[P2P] no pending offers");
            return;
        }

        Console.WriteLine("[P2P] pending offers:");
        foreach (var o in offers)
            Console.WriteLine($"  from {o.PeerName} -> {o.Ip}:{o.Port} ({o.ReceivedAt:T})");
    }

    static void DenyOffer(string peer)
    {
        if (Offers.TryRemove(peer, out _))
            Console.WriteLine($"[P2P] denied offer from {peer}");
        else
            Console.WriteLine($"[P2P] no pending offer from '{peer}'");
    }

    static void ClosePeer(string name)
    {
        if (Peers.TryRemove(name, out var pc))
        {
            try { pc.Writer.WriteLine("BYE"); } catch { }
            pc.Close();
            Console.WriteLine($"[P2P] closed {name}");
        }
        else Console.WriteLine($"[P2P] unknown peer '{name}'");
    }

    static void CloseAllPeers()
    {
        foreach (var k in Peers.Keys.ToArray())
            ClosePeer(k);
    }

    static async Task SendToPeer(string name, string text)
    {
        if (!Peers.TryGetValue(name, out var pc))
        {
            Console.WriteLine($"[P2P] unknown peer '{name}'");
            return;
        }

        try
        {
            await pc.Writer.WriteLineAsync(text);
        }
        catch
        {
            Console.WriteLine($"[P2P] send failed, removing {name}");
            Peers.TryRemove(name, out _);
            pc.Close();
        }
    }

    static async Task ServerRecvLoopAsync(StreamReader serverReader)
    {
        try
        {
            while (true)
            {
                var line = await serverReader.ReadLineAsync();
                if (line == null) break;

                line = line.TrimEnd('\r', '\n');

                if (line.StartsWith("#p2p_offer ", StringComparison.Ordinal))
                {
                    // "#p2p_offer <peerName> <ip> <port>"
                    var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length == 4 && int.TryParse(parts[3], out int port))
                    {
                        string peerName = parts[1];
                        string ip = parts[2];

                        var offer = new Offer(peerName, ip, port, DateTime.Now);
                        Offers[peerName] = offer;

                        Console.WriteLine($"[P2P] offer from {peerName} -> {ip}:{port}");
                        Console.WriteLine($"      type: /accept {peerName}  or  /deny {peerName}");
                        continue;
                    }
                }

                Console.WriteLine("<SERVER> " + line);
            }
        }
        catch { }
        Console.WriteLine("Verbindung zum Server getrennt");
    }

    static async Task AcceptPeersLoopAsync(TcpListener listener)
    {
        while (true)
        {
            TcpClient c;
            try { c = await listener.AcceptTcpClientAsync(); }
            catch { break; }

            _ = Task.Run(() => HandleIncomingPeerAsync(c));
        }
    }

    static async Task HandleIncomingPeerAsync(TcpClient c)
    {
        PeerConn? pc = null;
        try
        {
            var ns = c.GetStream();
            var r = new StreamReader(ns, Encoding.UTF8, leaveOpen: true);
            var w = new StreamWriter(ns, Encoding.UTF8) { AutoFlush = true };

            // expect HELLO <name>
            var hello = await r.ReadLineAsync();
            var peerName = ParseHelloName(hello) ?? "peer";

            // send back HELLO <me>
            await w.WriteLineAsync($"HELLO {MyName}");

            pc = new PeerConn(c, peerName);
            Peers[peerName] = pc;

            // offer kann weg, falls noch vorhanden
            Offers.TryRemove(peerName, out _);

            Console.WriteLine($"[P2P] incoming connection from {peerName}");

            await PeerRecvLoopAsync(pc);
        }
        catch
        {
            pc?.Close();
        }
    }

    static async Task ConnectToPeerAsync(string peerNameFromOffer, string ip, int port)
    {
        try
        {
            var c = new TcpClient();
            await c.ConnectAsync(IPAddress.Parse(ip), port);

            var ns = c.GetStream();
            var r = new StreamReader(ns, Encoding.UTF8, leaveOpen: true);
            var w = new StreamWriter(ns, Encoding.UTF8) { AutoFlush = true };

            // send HELLO <me>
            await w.WriteLineAsync($"HELLO {MyName}");

            // expect HELLO <peer>
            var back = await r.ReadLineAsync();
            var realName = ParseHelloName(back) ?? peerNameFromOffer;

            var pc = new PeerConn(c, realName);
            Peers[realName] = pc;

            Console.WriteLine($"[P2P] connected to {realName} at {ip}:{port}");

            await PeerRecvLoopAsync(pc);
        }
        catch
        {
            Console.WriteLine($"[P2P] connect failed to {ip}:{port}");
        }
    }

    static async Task PeerRecvLoopAsync(PeerConn pc)
    {
        try
        {
            while (true)
            {
                var line = await pc.Reader.ReadLineAsync();
                if (line == null) break;

                line = line.TrimEnd('\r', '\n');
                if (line == "BYE")
                {
                    Console.WriteLine($"[P2P {pc.Name}] peer closed");
                    break;
                }

                Console.WriteLine($"[P2P {pc.Name}] {line}");
            }
        }
        catch { }
        finally
        {
            Peers.TryRemove(pc.Name, out _);
            pc.Close();
        }
    }

    static string? ParseHelloName(string? line)
    {
        if (line == null) return null;
        line = line.Trim();
        if (line.StartsWith("HELLO ", StringComparison.OrdinalIgnoreCase))
        {
            var name = line.Substring(6).Trim();
            return name.Length > 0 ? name : null;
        }
        return null;
    }
}
