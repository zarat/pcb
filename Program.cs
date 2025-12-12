using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

public class Program
{
    public static async Task Main(string[] args)
    {
        if (args.Length == 0)
        {
            PrintUsage();
            return;
        }

        switch (args[0].ToLowerInvariant())
        {
            case "server":
            {
                if (args.Length < 2) { PrintUsage(); return; }
                await Program1.RunAsync(new[] { args[1] });
                break;
            }

            case "client":
            {
                // online: client <server_ip> <server_port> <my_p2p_port>
                // offline: client <my_p2p_port>
                if (args.Length == 2)
                {
                    await Program2.RunAsync(new[] { args[1] }); // offline
                }
                else if (args.Length >= 4)
                {
                    await Program2.RunAsync(new[] { args[1], args[2], args[3] }); // online
                }
                else
                {
                    PrintUsage();
                }
                break;
            }

            default:
                PrintUsage();
                break;
        }
    }

    static void PrintUsage()
    {
        Console.WriteLine("Usage:");
        Console.WriteLine("  app server <port>");
        Console.WriteLine("  app client <server_ip> <server_port> <my_p2p_port>");
        Console.WriteLine("  app client <my_p2p_port>                (offline P2P only)");
        Console.WriteLine();
        Console.WriteLine("Examples:");
        Console.WriteLine("  app server 27015");
        Console.WriteLine("  app client 127.0.0.1 27015 27016");
        Console.WriteLine("  app client 27016");
    }
}

// ========================= SERVER =========================

sealed class ClientState
{
    static readonly Encoding Utf8NoBom = new UTF8Encoding(false);

    public TcpClient Client { get; }
    public StreamReader Reader { get; }
    public StreamWriter Writer { get; }
    public string Username { get; set; } = "";
    public IPAddress RemoteIp { get; }

    public ClientState(TcpClient c)
    {
        Client = c;
        var ns = c.GetStream();
        Reader = new StreamReader(ns, Utf8NoBom, leaveOpen: true);
        Writer = new StreamWriter(ns, Utf8NoBom) { AutoFlush = true };
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

        if (!int.TryParse(args[0], out int port) || port <= 0 || port > 65535)
        {
            Console.WriteLine("Invalid port.");
            return;
        }

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

// ========================= CLIENT / P2P =========================

sealed class PeerConn
{
    static readonly Encoding Utf8NoBom = new UTF8Encoding(false);

    public TcpClient Client { get; }
    public StreamReader Reader { get; }
    public StreamWriter Writer { get; }

    public string Name { get; set; }
    public IPAddress Ip { get; }
    public int ListenPort { get; set; }

    public PeerConn(TcpClient c, string name, IPAddress ip, int listenPort)
    {
        Client = c;
        var ns = c.GetStream();
        Reader = new StreamReader(ns, Utf8NoBom, leaveOpen: true);
        Writer = new StreamWriter(ns, Utf8NoBom) { AutoFlush = true };
        Name = name;
        Ip = ip;
        ListenPort = listenPort;
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
    static readonly Encoding Utf8NoBom = new UTF8Encoding(false);

    static readonly ConcurrentDictionary<string, PeerConn> Peers =
        new ConcurrentDictionary<string, PeerConn>(StringComparer.OrdinalIgnoreCase);

    static readonly ConcurrentDictionary<string, Offer> Offers =
        new ConcurrentDictionary<string, Offer>(StringComparer.OrdinalIgnoreCase);

    static string MyName = "anon";
    static int MyP2pPort;

    static volatile bool ServerOnline = false;
    static TcpClient? ServerClient;
    static StreamReader? ServerReader;
    static StreamWriter? ServerWriter;

    // ===================== Remote-DIAG (/cmd) =====================
    // Protocol over peer stream:
    //   CMDREQ <id> <b64(cmdline)>
    //   CMDACK <id> OK|NO
    //   CMDOUT <id> <b64(chunk)>
    //   CMDEND <id>

    sealed record CmdReq(string Id, string Peer, string CmdLine, DateTime At);
    static readonly ConcurrentDictionary<string, CmdReq> OutgoingCmd = new(StringComparer.OrdinalIgnoreCase);
    static readonly ConcurrentDictionary<string, CmdReq> IncomingCmd = new(StringComparer.OrdinalIgnoreCase);
    static readonly ConcurrentDictionary<string, CancellationTokenSource> RunningCmd = new(StringComparer.OrdinalIgnoreCase);

    static readonly HttpClient Http = new HttpClient(new HttpClientHandler { AllowAutoRedirect = true })
    {
        Timeout = TimeSpan.FromSeconds(12)
    };

    static string ToB64(string s) => Convert.ToBase64String(Utf8NoBom.GetBytes(s));
    static string FromB64(string b64) => Utf8NoBom.GetString(Convert.FromBase64String(b64));

    public static async Task RunAsync(string[] args)
    {
        // offline: [my_p2p_port]
        // online : [server_ip, server_port, my_p2p_port]
        string? serverIp = null;
        int serverPort = 0;

        if (args.Length == 1)
        {
            if (!int.TryParse(args[0], out MyP2pPort) || MyP2pPort <= 0 || MyP2pPort > 65535)
            {
                Console.WriteLine("Invalid P2P port.");
                return;
            }
        }
        else if (args.Length >= 3)
        {
            serverIp = args[0];
            if (!int.TryParse(args[1], out serverPort) || serverPort <= 0 || serverPort > 65535)
            {
                Console.WriteLine("Invalid server port.");
                return;
            }
            if (!int.TryParse(args[2], out MyP2pPort) || MyP2pPort <= 0 || MyP2pPort > 65535)
            {
                Console.WriteLine("Invalid P2P port.");
                return;
            }
        }
        else
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  Client <server_ip> <server_port> <my_p2p_port>");
            Console.WriteLine("  Client <my_p2p_port>   (offline P2P only)");
            return;
        }

        // P2P listener IMMER starten
        var p2pListener = new TcpListener(IPAddress.Any, MyP2pPort);
        p2pListener.Start();
        _ = Task.Run(() => AcceptIncomingLoopAsync(p2pListener));
        Console.WriteLine($"[P2P] listening on 0.0.0.0:{MyP2pPort}");

        // Username
        Console.Write("Username: ");
        MyName = (Console.ReadLine() ?? "anon").Trim();
        if (MyName.Length == 0) MyName = "anon";

        // optional: Server connect beim Start
        if (serverIp != null)
            await TryConnectServerAsync(serverIp, serverPort);

        if (!ServerOnline)
        {
            if (serverIp != null)
                Console.WriteLine("[INFO] OFFLINE-P2P Modus aktiv (Server nicht verbunden).");
            else
                Console.WriteLine("[INFO] OFFLINE-P2P Modus (kein Server angegeben).");
        }

        PrintHelp();

        while (true)
        {
            var raw = Console.ReadLine();
            if (raw == null) break;

            var line = raw.Trim();
            if (line.Length == 0) continue;

            if (line.Equals("/help", StringComparison.OrdinalIgnoreCase)) { PrintHelp(); continue; }
            if (line.Equals("/plist", StringComparison.OrdinalIgnoreCase)) { PrintPeers(); continue; }
            if (line.Equals("/offers", StringComparison.OrdinalIgnoreCase)) { PrintOffers(); continue; }
            if (line.Equals("/pcloseall", StringComparison.OrdinalIgnoreCase)) { CloseAllPeers(); continue; }

            if (line.Equals("/cmdpending", StringComparison.OrdinalIgnoreCase))
            {
                PrintCmdPending();
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

            // Direkt verbinden (ohne Offer)
            if (line.StartsWith("/connect ", StringComparison.OrdinalIgnoreCase))
            {
                // /connect <ip> <port> [name]
                var p = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (p.Length < 3 || !int.TryParse(p[2], out int port) || port <= 0 || port > 65535)
                {
                    Console.WriteLine("[P2P] Usage: /connect <ip> <port> [name]");
                    continue;
                }

                string ip = p[1];
                string suggestedName = p.Length >= 4 ? p[3] : ip;

                if (Peers.ContainsKey(suggestedName))
                {
                    Console.WriteLine($"[P2P] already connected as '{suggestedName}' (try different name or close first)");
                    continue;
                }

                _ = Task.Run(() => ConnectToPeerAsync(suggestedName, ip, port));
                continue;
            }

            // Offline Offer (Empfänger muss /accept machen)
            if (line.StartsWith("/poffer ", StringComparison.OrdinalIgnoreCase))
            {
                // /poffer <ip> <port>
                var p = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (p.Length < 3 || !int.TryParse(p[2], out int port) || port <= 0 || port > 65535)
                {
                    Console.WriteLine("[P2P] Usage: /poffer <ip> <port>");
                    continue;
                }

                string ip = p[1];
                _ = Task.Run(() => SendLocalOfferAsync(ip, port));
                continue;
            }

            // -------- /cmd: request diag on peer (peer must confirm via /cmdallow) --------
            if (line.StartsWith("/cmd ", StringComparison.OrdinalIgnoreCase))
            {
                // /cmd <peer> <cmdline>
                var p = line.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
                if (p.Length < 3)
                {
                    Console.WriteLine("[CMD] Usage: /cmd <peer> <cmdline>");
                    Console.WriteLine("[CMD] cmdline: dns|ping|tcp|http|trace|ifaces");
                    continue;
                }

                var peer = p[1];
                var cmdLine = p[2];

                if (!Peers.ContainsKey(peer))
                {
                    Console.WriteLine($"[CMD] unknown peer '{peer}'");
                    continue;
                }

				/*
                if (!IsSupportedDiagCommand(cmdLine, out var why))
                {
                    Console.WriteLine($"[CMD] blocked/unknown command: {why}");
                    Console.WriteLine("[CMD] supported: dns, ping, tcp, http, trace, ifaces");
                    continue;
                }
				*/

                var id = Guid.NewGuid().ToString("N");
                OutgoingCmd[id] = new CmdReq(id, peer, cmdLine, DateTime.Now);

                await SendToPeer(peer, $"CMDREQ {id} {ToB64(cmdLine)}");
                Console.WriteLine($"[CMD] request sent -> {peer} id={id}: {cmdLine}");
                continue;
            }

            // -------- customer confirm/deny incoming cmd --------
            if (line.StartsWith("/cmdallow ", StringComparison.OrdinalIgnoreCase))
            {
                var id = line.Substring(10).Trim();
                if (id.Length == 0) { Console.WriteLine("[CMD] Usage: /cmdallow <id>"); continue; }

                if (!IncomingCmd.TryRemove(id, out var req))
                {
                    Console.WriteLine($"[CMD] no pending incoming cmd with id={id}");
                    continue;
                }

                if (!Peers.TryGetValue(req.Peer, out var pc))
                {
                    Console.WriteLine($"[CMD] peer '{req.Peer}' not connected anymore");
                    continue;
                }

                await SafeWriteAsync(pc, $"CMDACK {id} OK");

                // run diag in background and stream output
                _ = Task.Run(() => ExecuteAndStreamDiagAsync(pc, id, req.CmdLine));
                Console.WriteLine($"[CMD] allowed id={id} from {req.Peer}: {req.CmdLine}");
                continue;
            }

            if (line.StartsWith("/cmddeny ", StringComparison.OrdinalIgnoreCase))
            {
                var id = line.Substring(9).Trim();
                if (id.Length == 0) { Console.WriteLine("[CMD] Usage: /cmddeny <id>"); continue; }

                if (!IncomingCmd.TryRemove(id, out var req))
                {
                    Console.WriteLine($"[CMD] no pending incoming cmd with id={id}");
                    continue;
                }

                if (Peers.TryGetValue(req.Peer, out var pc))
                    await SafeWriteAsync(pc, $"CMDACK {id} NO");

                Console.WriteLine($"[CMD] denied id={id} from {req.Peer}: {req.CmdLine}");
                continue;
            }

            // -------- support abort request --------
            if (line.StartsWith("/cmdend ", StringComparison.OrdinalIgnoreCase))
            {
                // /cmdend <peer> <id>
                var p = line.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
                if (p.Length < 3) { Console.WriteLine("[CMD] Usage: /cmdend <peer> <id>"); continue; }

                await SendToPeer(p[1], $"CMDEND {p[2]}");
                Console.WriteLine($"[CMD] end sent to {p[1]} id={p[2]}");
                continue;
            }
            // ---------------------------------------------------------------------------

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

            // Datei senden via Peer
            if (line.StartsWith("/fsend ", StringComparison.OrdinalIgnoreCase))
            {
                // /fsend <peer> <filepath>
                var p = line.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
                if (p.Length < 3)
                {
                    Console.WriteLine("[FILE] Usage: /fsend <peer> <filepath>");
                    continue;
                }

                var peer = p[1];
                var path = p[2].Trim('"');

                if (!Peers.TryGetValue(peer, out var pc))
                {
                    Console.WriteLine($"[FILE] unknown peer '{peer}'");
                    continue;
                }

                if (pc.ListenPort <= 0)
                {
                    Console.WriteLine($"[FILE] peer '{peer}' has no listen port info (reconnect needed).");
                    continue;
                }

                _ = Task.Run(() => SendFileToAsync(pc.Ip.ToString(), pc.ListenPort, path));
                continue;
            }

            // Datei direkt senden
            if (line.StartsWith("/fsendip ", StringComparison.OrdinalIgnoreCase))
            {
                // /fsendip <ip> <port> <filepath>
                var p = line.Split(' ', 4, StringSplitOptions.RemoveEmptyEntries);
                if (p.Length < 4 || !int.TryParse(p[2], out int port) || port <= 0 || port > 65535)
                {
                    Console.WriteLine("[FILE] Usage: /fsendip <ip> <port> <filepath>");
                    continue;
                }
                var ip = p[1];
                var path = p[3].Trim('"');
                _ = Task.Run(() => SendFileToAsync(ip, port, path));
                continue;
            }

            // Server verbinden während Laufzeit
            if (line.StartsWith("/sconnect ", StringComparison.OrdinalIgnoreCase))
            {
                var p = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (p.Length != 3 || !int.TryParse(p[2], out int sp) || sp <= 0 || sp > 65535)
                {
                    Console.WriteLine("[SERVER] Usage: /sconnect <ip> <port>");
                    continue;
                }
                await TryConnectServerAsync(p[1], sp);
                continue;
            }

            // Server trennen
            if (line.Equals("/sdisconnect", StringComparison.OrdinalIgnoreCase))
            {
                DisconnectServer("[SERVER] disconnected by user.");
                continue;
            }

            // default -> server (nur wenn online)
            if (ServerOnline && ServerWriter != null)
            {
                try
                {
                    await ServerWriter.WriteLineAsync(line);
                }
                catch
                {
                    DisconnectServer("[WARN] Server connection lost. OFFLINE-P2P Modus aktiv.");
                }
            }
            else
            {
                if (line.StartsWith("#invite", StringComparison.OrdinalIgnoreCase))
                    Console.WriteLine("[WARN] #invite geht nur mit Server. Offline: /poffer <ip> <port> oder /connect <ip> <port> [name]");
                else
                    Console.WriteLine("[INFO] Kein Server verbunden. Nutze /connect oder /poffer oder /p (oder /help).");
            }
        }
    }

    static void PrintHelp()
    {
        Console.WriteLine("Commands:");
        Console.WriteLine("  #invite <user> <port>               (nur wenn Server online)");
        Console.WriteLine("  #userlist                           (nur wenn Server online)");
        Console.WriteLine("  /sconnect <ip> <port>               (Server verbinden während Laufzeit)");
        Console.WriteLine("  /sdisconnect                        (Serversession beenden)");
        Console.WriteLine("  /connect <ip> <port> [name]         (direkt P2P verbinden, ohne Offer)");
        Console.WriteLine("  /poffer <ip> <port>                 (offline Offer schicken -> Empfänger /accept)");
        Console.WriteLine("  /offers                             (eingehende Offers anzeigen)");
        Console.WriteLine("  /accept <user>                      (Offer annehmen -> P2P verbinden)");
        Console.WriteLine("  /deny <user>                        (Offer ablehnen)");
        Console.WriteLine("  /p <peer> <text>                    (P2P Nachricht)");
        Console.WriteLine("  /cmd <peer> <cmdline>               (Remote-DIAG Anfrage; Empfänger MUSS bestätigen)");
        Console.WriteLine("      cmdline: dns|ping|tcp|http|trace|ifaces");
        Console.WriteLine("  /cmdpending                         (zeigt offene eingehende CMDREQs)");
        Console.WriteLine("  /cmdallow <id>                      (eingehende CMDREQ erlauben)");
        Console.WriteLine("  /cmddeny <id>                       (eingehende CMDREQ ablehnen)");
        Console.WriteLine("  /cmdend <peer> <id>                 (Remote-DIAG abbrechen)");
        Console.WriteLine("  /fsend <peer> <filepath>            (Datei an verbundenen Peer senden)");
        Console.WriteLine("  /fsendip <ip> <port> <filepath>     (Datei direkt an IP/Port senden)");
        Console.WriteLine("  /plist                              (aktive Peers)");
        Console.WriteLine("  /pclose <peer>                      (Peer schließen)");
        Console.WriteLine("  /pcloseall                          (alle schließen)");
        Console.WriteLine("  /help");
        Console.WriteLine();
        Console.WriteLine("CMD examples:");
        Console.WriteLine("  /cmd Bob dns example.com");
        Console.WriteLine("  /cmd Bob ping example.com 4");
        Console.WriteLine("  /cmd Bob tcp example.com 443");
        Console.WriteLine("  /cmd Bob http https://example.com");
        Console.WriteLine("  /cmd Bob trace example.com 20");
        Console.WriteLine("  /cmd Bob ifaces");
    }

    static void PrintCmdPending()
    {
        var arr = IncomingCmd.Values.OrderBy(x => x.At).ToArray();
        if (arr.Length == 0)
        {
            Console.WriteLine("[CMD] no pending incoming requests");
            return;
        }

        Console.WriteLine("[CMD] pending incoming:");
        foreach (var r in arr)
            Console.WriteLine($"  id={r.Id} from={r.Peer} cmd='{r.CmdLine}' at={r.At:T}");
    }

    // ================= SERVER CONNECT/DISCONNECT =================

    static async Task TryConnectServerAsync(string serverIp, int serverPort)
    {
        if (ServerOnline)
        {
            Console.WriteLine("[SERVER] already connected.");
            return;
        }

        try
        {
            ServerClient = new TcpClient();
            await ServerClient.ConnectAsync(IPAddress.Parse(serverIp), serverPort);

            var ns = ServerClient.GetStream();
            ServerReader = new StreamReader(ns, Utf8NoBom, leaveOpen: true);
            ServerWriter = new StreamWriter(ns, Utf8NoBom) { AutoFlush = true };

            var prompt = await ServerReader.ReadLineAsync();
            if (prompt != null) Console.WriteLine($"<SERVER> {prompt}");

            await ServerWriter.WriteLineAsync(MyName);

            ServerOnline = true;
            Console.WriteLine("[SERVER] connected.");

            _ = Task.Run(() => ServerRecvLoopAsync());
        }
        catch (Exception ex)
        {
            DisconnectServer($"[WARN] Server nicht erreichbar ({ex.Message}). OFFLINE-P2P Modus aktiv.");
        }
    }

    static void DisconnectServer(string msg)
    {
        Console.WriteLine(msg);
        ServerOnline = false;
        try { ServerClient?.Close(); } catch { }
        ServerClient = null;
        ServerReader = null;
        ServerWriter = null;
    }

    static async Task ServerRecvLoopAsync()
    {
        try
        {
            if (ServerReader == null) return;

            while (true)
            {
                var line = await ServerReader.ReadLineAsync();
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

                        Offers[peerName] = new Offer(peerName, ip, port, DateTime.Now);

                        Console.WriteLine($"[P2P] offer from {peerName} -> {ip}:{port}");
                        Console.WriteLine($"      type: /accept {peerName}  or  /deny {peerName}");
                        continue;
                    }
                }

                Console.WriteLine("<SERVER> " + line);
            }
        }
        catch { }

        DisconnectServer("[WARN] Verbindung zum Server getrennt. OFFLINE-P2P Modus aktiv.");
    }

    // ================= PEERS / OFFERS =================

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

    // ================= INCOMING MULTIPLEX (CHAT / OFFER / FILE / CMD) =================

    static async Task AcceptIncomingLoopAsync(TcpListener listener)
    {
        while (true)
        {
            TcpClient c;
            try { c = await listener.AcceptTcpClientAsync(); }
            catch { break; }

            _ = Task.Run(() => HandleIncomingConnectionAsync(c));
        }
    }

    static async Task HandleIncomingConnectionAsync(TcpClient c)
    {
        try
        {
            var ns = c.GetStream();

            // erste Zeile "roh" lesen (damit wir entscheiden können: FILE / OFFER / HELLO)
            var first = await ReadLineAsync(ns);
            if (first == null) { c.Close(); return; }

            if (first.StartsWith("FILEHELLO ", StringComparison.OrdinalIgnoreCase))
            {
                var sender = first.Substring(9).Trim();
                if (sender.Length == 0) sender = "peer";
                await HandleIncomingFileAsync(c, sender);
                return;
            }

            if (first.StartsWith("OFFER ", StringComparison.OrdinalIgnoreCase))
            {
                await HandleIncomingOfferAsync(c, first);
                return;
            }

            if (first.StartsWith("HELLO ", StringComparison.OrdinalIgnoreCase))
            {
                await HandleIncomingPeerAsync(c, first);
                return;
            }

            c.Close();
        }
        catch
        {
            try { c.Close(); } catch { }
        }
    }

    static async Task HandleIncomingOfferAsync(TcpClient c, string firstLine)
    {
        try
        {
            // OFFER <name> <port>
            var parts = firstLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var ns = c.GetStream();

            if (parts.Length >= 3 && int.TryParse(parts[2], out int p2pPort) && p2pPort > 0 && p2pPort <= 65535)
            {
                var remoteIp = ((IPEndPoint)c.Client.RemoteEndPoint!).Address.ToString();
                var peerName = parts[1];

                Offers[peerName] = new Offer(peerName, remoteIp, p2pPort, DateTime.Now);

                await WriteLineAsync(ns, "OK");
                Console.WriteLine($"[P2P] offer from {peerName} -> {remoteIp}:{p2pPort}");
                Console.WriteLine($"      type: /accept {peerName}  or  /deny {peerName}");
            }
            else
            {
                await WriteLineAsync(ns, "NO");
            }

            c.Close();
        }
        catch
        {
            try { c.Close(); } catch { }
        }
    }

    static async Task SendLocalOfferAsync(string ip, int port)
    {
        try
        {
            using var c = new TcpClient();
            await c.ConnectAsync(IPAddress.Parse(ip), port);
            var ns = c.GetStream();

            await WriteLineAsync(ns, $"OFFER {MyName} {MyP2pPort}");
            var resp = await ReadLineAsync(ns);

            Console.WriteLine($"[P2P] offer sent to {ip}:{port} (resp={resp ?? "null"})");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[P2P] offer failed ({ex.Message})");
        }
    }

    static async Task HandleIncomingPeerAsync(TcpClient c, string helloLineAlreadyRead)
    {
        PeerConn? pc = null;
        try
        {
            var remoteIp = ((IPEndPoint)c.Client.RemoteEndPoint!).Address;
            var (peerName, peerListenPort) = ParseHello(helloLineAlreadyRead);
            peerName ??= "peer";

            var ns = c.GetStream();

            // send back HELLO <me> <myPort>
            await WriteLineAsync(ns, $"HELLO {MyName} {MyP2pPort}");

            pc = new PeerConn(c, peerName, remoteIp, peerListenPort);

            if (!Peers.TryAdd(peerName, pc))
            {
                await WriteLineAsync(ns, "BYE");
                pc.Close();
                Console.WriteLine($"[P2P] incoming '{peerName}' rejected (name already connected)");
                return;
            }

            Offers.TryRemove(peerName, out _);
            Console.WriteLine($"[P2P] incoming connection from {peerName} ({remoteIp}) listenPort={peerListenPort}");

            await PeerRecvLoopAsync(pc);
        }
        catch
        {
            pc?.Close();
        }
    }

    static async Task ConnectToPeerAsync(string peerNameFromOfferOrSuggested, string ip, int port)
    {
        try
        {
            var c = new TcpClient();
            await c.ConnectAsync(IPAddress.Parse(ip), port);

            var ns = c.GetStream();

            // send HELLO <me> <myPort>
            await WriteLineAsync(ns, $"HELLO {MyName} {MyP2pPort}");

            // expect HELLO <peer> <peerPort>
            var back = await ReadLineAsync(ns);
            var (realName, peerListenPort) = ParseHello(back);
            realName ??= peerNameFromOfferOrSuggested;

            if (peerListenPort <= 0) peerListenPort = port;

            var remoteIp = ((IPEndPoint)c.Client.RemoteEndPoint!).Address;

            var pc = new PeerConn(c, realName, remoteIp, peerListenPort);

            if (!Peers.TryAdd(realName, pc))
            {
                try { await WriteLineAsync(ns, "BYE"); } catch { }
                pc.Close();
                Console.WriteLine($"[P2P] connect ok but name '{realName}' already exists -> closed");
                return;
            }

            Offers.TryRemove(realName, out _);

            Console.WriteLine($"[P2P] connected to {realName} at {ip}:{port} listenPort={pc.ListenPort}");
            await PeerRecvLoopAsync(pc);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[P2P] connect failed to {ip}:{port} ({ex.Message})");
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

                if (line.StartsWith("CMDREQ ", StringComparison.OrdinalIgnoreCase))
                {
                    HandleCmdReq(pc, line);
                    continue;
                }
                if (line.StartsWith("CMDACK ", StringComparison.OrdinalIgnoreCase))
                {
                    HandleCmdAck(pc, line);
                    continue;
                }
                if (line.StartsWith("CMDOUT ", StringComparison.OrdinalIgnoreCase))
                {
                    HandleCmdOut(pc, line);
                    continue;
                }
                if (line.StartsWith("CMDEND ", StringComparison.OrdinalIgnoreCase))
                {
                    HandleCmdEnd(pc, line);
                    continue;
                }

                Console.WriteLine($"[P2P {pc.Name}] {line}");
            }
        }
        catch { }
        finally
        {
            // cleanup pending incoming/outgoing related to this peer
            foreach (var kv in IncomingCmd.Where(kv => kv.Value.Peer.Equals(pc.Name, StringComparison.OrdinalIgnoreCase)).ToArray())
                IncomingCmd.TryRemove(kv.Key, out _);
            foreach (var kv in OutgoingCmd.Where(kv => kv.Value.Peer.Equals(pc.Name, StringComparison.OrdinalIgnoreCase)).ToArray())
                OutgoingCmd.TryRemove(kv.Key, out _);

            Peers.TryRemove(pc.Name, out _);
            pc.Close();
        }
    }

    static (string? Name, int Port) ParseHello(string? line)
    {
        if (string.IsNullOrWhiteSpace(line)) return (null, 0);
        line = line.Trim();

        if (!line.StartsWith("HELLO ", StringComparison.OrdinalIgnoreCase))
            return (null, 0);

        var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length >= 2)
        {
            var name = parts[1].Trim();
            int port = 0;
            if (parts.Length >= 3) int.TryParse(parts[2], out port);
            return (name.Length > 0 ? name : null, port);
        }
        return (null, 0);
    }

    // ===================== CMD implementation =====================

    static bool IsSupportedDiagCommand(string cmdLine, out string reason)
    {
        reason = "";
        if (string.IsNullOrWhiteSpace(cmdLine)) { reason = "empty"; return false; }
        if (cmdLine.Length > 300) { reason = "too long"; return false; }

        var parts = cmdLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var verb = parts[0].ToLowerInvariant();

        switch (verb)
        {
            case "dns":
                if (parts.Length < 2) { reason = "dns <host>"; return false; }
                return true;
            case "ping":
                if (parts.Length < 2) { reason = "ping <host> [count]"; return false; }
                return true;
            case "tcp":
                if (parts.Length < 3 || !int.TryParse(parts[2], out var port) || port <= 0 || port > 65535)
                { reason = "tcp <host> <port>"; return false; }
                return true;
            case "http":
                if (parts.Length < 2) { reason = "http <url>"; return false; }
                return true;
            case "trace":
                if (parts.Length < 2) { reason = "trace <host> [maxHops]"; return false; }
                return true;
            case "ifaces":
                return true;
            default:
                reason = "unknown verb";
                return false;
        }
    }

    static void HandleCmdReq(PeerConn pc, string line)
    {
        // CMDREQ <id> <b64(cmdline)>
        var parts = line.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 3) return;

        var id = parts[1];
        string cmdLine;
        try { cmdLine = FromB64(parts[2]); }
        catch
        {
            _ = SafeWriteAsync(pc, $"CMDACK {id} NO");
            return;
        }
		
		/*
        if (!IsSupportedDiagCommand(cmdLine, out var why))
        {
            _ = SafeWriteAsync(pc, $"CMDACK {id} NO");
            Console.WriteLine($"[CMD] blocked incoming from {pc.Name}: {cmdLine} ({why})");
            return;
        }
		*/

        IncomingCmd[id] = new CmdReq(id, pc.Name, cmdLine, DateTime.Now);
        Console.WriteLine($"[CMD] incoming request from {pc.Name}: id={id} cmd='{cmdLine}'");
        Console.WriteLine($"      type: /cmdallow {id}  or  /cmddeny {id}");
    }

    static void HandleCmdAck(PeerConn pc, string line)
    {
        // CMDACK <id> OK|NO
        var parts = line.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 3) return;

        var id = parts[1];
        var ok = parts[2].Equals("OK", StringComparison.OrdinalIgnoreCase);

        if (OutgoingCmd.TryGetValue(id, out var req))
        {
            Console.WriteLine(ok
                ? $"[CMD] accepted by {pc.Name} id={id}: {req.CmdLine}"
                : $"[CMD] denied by {pc.Name} id={id}: {req.CmdLine}");

            if (!ok) OutgoingCmd.TryRemove(id, out _);
        }
        else
        {
            Console.WriteLine($"[CMD] ack from {pc.Name}: id={id} {(ok ? "OK" : "NO")}");
        }
    }

    static void HandleCmdOut(PeerConn pc, string line)
    {
        // CMDOUT <id> <b64(chunk)>
        var parts = line.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 3) return;

        var id = parts[1];
        string text;
        try { text = FromB64(parts[2]); }
        catch { text = "[invalid output chunk]"; }

        Console.WriteLine($"[CMDOUT {pc.Name} id={id}] {text}");
    }

    static void HandleCmdEnd(PeerConn pc, string line)
    {
        // CMDEND <id>
        var parts = line.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2) return;

        var id = parts[1];

        if (RunningCmd.TryRemove(id, out var cts))
        {
            try { cts.Cancel(); } catch { }
            cts.Dispose();
            Console.WriteLine($"[CMD] cancelled by peer {pc.Name} id={id}");
        }

        if (OutgoingCmd.TryRemove(id, out var req))
            Console.WriteLine($"[CMD] ended by {pc.Name} id={id} (cmd: {req.CmdLine})");
        else
            Console.WriteLine($"[CMD] end from {pc.Name} id={id}");
    }

    static async Task ExecuteAndStreamDiagAsync(PeerConn pc, string id, string cmdLine)
    {
        var cts = new CancellationTokenSource();
        if (!RunningCmd.TryAdd(id, cts))
        {
            await SafeWriteAsync(pc, $"CMDEND {id}");
            return;
        }

        try
        {
            var output = await RunDiagAsync(cmdLine, cts.Token);
            await SendCmdOutChunkedAsync(pc, id, output);
        }
        catch (OperationCanceledException)
        {
            await SendCmdOutChunkedAsync(pc, id, "[cancelled]");
        }
        catch (Exception ex)
        {
            await SendCmdOutChunkedAsync(pc, id, "[error] " + ex.Message);
        }
        finally
        {
            if (RunningCmd.TryRemove(id, out var old))
                old.Dispose();

            await SafeWriteAsync(pc, $"CMDEND {id}");
        }
    }

    static async Task SafeWriteAsync(PeerConn pc, string line)
    {
        try { await pc.Writer.WriteLineAsync(line); } catch { }
    }

    static async Task SendCmdOutChunkedAsync(PeerConn pc, string id, string output)
    {
        if (output.Length > 32_000) output = output.Substring(0, 32_000) + "\n[truncated]";

        var bytes = Utf8NoBom.GetBytes(output);
        const int chunk = 6000;

        for (int i = 0; i < bytes.Length; i += chunk)
        {
            int n = Math.Min(chunk, bytes.Length - i);
            var b64 = Convert.ToBase64String(bytes, i, n);
            await SafeWriteAsync(pc, $"CMDOUT {id} {b64}");
        }
    }

    static async Task<string> RunDiagAsync(string cmdLine, CancellationToken ct)
    {
		
		
        var parts = cmdLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var verb = parts[0].ToLowerInvariant();

        switch (verb)
        {
            case "dns1":
                return await DiagDnsAsync(parts[1], ct);

            case "ping1":
            {
                var host = parts[1];
                int count = 4;
                if (parts.Length >= 3) int.TryParse(parts[2], out count);
                if (count <= 0) count = 1;
                if (count > 10) count = 10;
                return await DiagPingAsync(host, count, ct);
            }

            case "tcp1":
            {
                var host = parts[1];
                int port = int.Parse(parts[2]);
                return await DiagTcpAsync(host, port, ct);
            }

            case "http1":
                return await DiagHttpAsync(parts[1], ct);

            case "trace1":
            {
                var host = parts[1];
                int maxHops = 20;
                if (parts.Length >= 3) int.TryParse(parts[2], out maxHops);
                if (maxHops <= 0) maxHops = 1;
                if (maxHops > 40) maxHops = 40;
                return await DiagTraceAsync(host, maxHops, ct);
            }

            case "ifaces1":
                return DiagIfaces();

            default:
                return DiagIfaces1(cmdLine); //"unknown diag command";
        }
		
    }

    static async Task<string> DiagDnsAsync(string host, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        try
        {
            var addrs = await Dns.GetHostAddressesAsync(host);
            var lines = addrs.Select(a => $"  {a}").ToArray();
            return lines.Length == 0 ? "dns: no addresses" : "dns:\n" + string.Join("\n", lines);
        }
        catch (Exception ex)
        {
            return "dns error: " + ex.Message;
        }
    }

    static async Task<string> DiagPingAsync(string host, int count, CancellationToken ct)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"ping {host} x{count}");

        using var ping = new Ping();
        for (int i = 1; i <= count; i++)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                var reply = await ping.SendPingAsync(host, 2000);
                sb.AppendLine($"  {i}: {reply.Status} time={reply.RoundtripTime}ms addr={reply.Address}");
            }
            catch (Exception ex)
            {
                sb.AppendLine($"  {i}: error {ex.Message}");
            }
        }
        return sb.ToString();
    }

    static async Task<string> DiagTcpAsync(string host, int port, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var c = new TcpClient();

        var connectTask = c.ConnectAsync(host, port);
        var timeoutTask = Task.Delay(4000, ct);

        var done = await Task.WhenAny(connectTask, timeoutTask);
        if (done == timeoutTask)
            return $"tcp {host}:{port} timeout";

        try
        {
            await connectTask;
            sw.Stop();
            return $"tcp {host}:{port} OK ({sw.ElapsedMilliseconds}ms)";
        }
        catch (Exception ex)
        {
            return $"tcp {host}:{port} FAIL ({ex.Message})";
        }
    }

    static async Task<string> DiagHttpAsync(string url, CancellationToken ct)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri))
            return "http: invalid url";

        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Head, uri);
            using var resp = await Http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct);

            var sb = new StringBuilder();
            sb.AppendLine($"http HEAD {uri}");
            sb.AppendLine($"status: {(int)resp.StatusCode} {resp.ReasonPhrase}");
            sb.AppendLine($"final: {resp.RequestMessage?.RequestUri}");

            foreach (var h in resp.Headers.Take(20))
                sb.AppendLine($"{h.Key}: {string.Join(", ", h.Value)}");

            if (resp.Content?.Headers != null)
            {
                foreach (var h in resp.Content.Headers.Take(20))
                    sb.AppendLine($"{h.Key}: {string.Join(", ", h.Value)}");
            }

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return "http error: " + ex.Message;
        }
    }

    static async Task<string> DiagTraceAsync(string host, int maxHops, CancellationToken ct)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"trace {host} maxHops={maxHops}");

        using var ping = new Ping();
        byte[] buffer = new byte[32];

        for (int ttl = 1; ttl <= maxHops; ttl++)
        {
            ct.ThrowIfCancellationRequested();

            PingReply reply;
            try
            {
                var opt = new PingOptions(ttl, true);
                reply = await ping.SendPingAsync(host, 2500, buffer, opt);
            }
            catch (Exception ex)
            {
                sb.AppendLine($"{ttl,2}: error {ex.Message}");
                continue;
            }

            var addr = reply.Address?.ToString() ?? "*";
            sb.AppendLine($"{ttl,2}: {reply.Status} {addr} time={reply.RoundtripTime}ms");

            if (reply.Status == IPStatus.Success)
                break;
        }

        return sb.ToString();
    }

    static string DiagIfaces()
    {
        var sb = new StringBuilder();
        sb.AppendLine("ifaces:");

        foreach (var ni in NetworkInterface.GetAllNetworkInterfaces())
        {
            sb.AppendLine($"- {ni.Name} ({ni.NetworkInterfaceType}) {ni.OperationalStatus}");
            try
            {
                var ip = ni.GetIPProperties();
                foreach (var ua in ip.UnicastAddresses)
                    sb.AppendLine($"    ip: {ua.Address}");
                foreach (var g in ip.GatewayAddresses)
                    sb.AppendLine($"    gw: {g.Address}");
                foreach (var d in ip.DnsAddresses)
                    sb.AppendLine($"    dns: {d}");
            }
            catch { }
        }

        return sb.ToString();
    }

	
	static string DiagIfaces1(string command)
    {

		var sb = new StringBuilder();

		var psi = new ProcessStartInfo
		{
			FileName = "cmd.exe",
			Arguments = "/c " + command,
			RedirectStandardOutput = true,
			RedirectStandardError = true,
			UseShellExecute = false,
			CreateNoWindow = true,
			// optional bei deutscher Ausgabe:
			// StandardOutputEncoding = Encoding.GetEncoding(850),
			// StandardErrorEncoding  = Encoding.GetEncoding(850),
		};

		using var process = new Process { StartInfo = psi };

		process.OutputDataReceived += (s, e) => { if (e.Data != null) sb.AppendLine(e.Data); };
		process.ErrorDataReceived  += (s, e) => { if (e.Data != null) sb.AppendLine(e.Data); };

		process.Start();
		process.BeginOutputReadLine();
		process.BeginErrorReadLine();

		process.WaitForExit();

        return sb.ToString();
    }


    // ================= FILE TRANSFER (separate connection) =================
    // Protocol:
    // Sender -> receiver:
    //   FILEHELLO <senderName>\n
    //   FILENAME <base64>\n
    //   FILESIZE <bytes>\n
    // Receiver -> sender:
    //   OK\n  (or NO\n)
    // Then sender streams <bytes> raw

    static async Task SendFileToAsync(string ip, int port, string filePath)
    {
        try
        {
            if (!File.Exists(filePath))
            {
                Console.WriteLine($"[FILE] not found: {filePath}");
                return;
            }

            var fi = new FileInfo(filePath);
            var fileName = fi.Name;
            long size = fi.Length;

            var c = new TcpClient();
            await c.ConnectAsync(IPAddress.Parse(ip), port);
            var ns = c.GetStream();

            await WriteLineAsync(ns, $"FILEHELLO {MyName}");
            await WriteLineAsync(ns, $"FILENAME {Convert.ToBase64String(Utf8NoBom.GetBytes(fileName))}");
            await WriteLineAsync(ns, $"FILESIZE {size}");

            var resp = await ReadLineAsync(ns);

            if (!string.Equals(resp, "OK", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"[FILE] rejected by receiver ({resp ?? "no response"})");
                c.Close();
                return;
            }

            Console.WriteLine($"[FILE] sending '{fileName}' ({size} bytes) -> {ip}:{port}");

            using (var fs = File.OpenRead(filePath))
            {
                await fs.CopyToAsync(ns);
                await ns.FlushAsync();
            }

            Console.WriteLine("[FILE] send complete.");
            c.Close();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FILE] send failed ({ex.Message})");
        }
    }

    static async Task HandleIncomingFileAsync(TcpClient c, string senderName)
    {
        try
        {
            var ns = c.GetStream();

            var fnLine = await ReadLineAsync(ns); // FILENAME ...
            var szLine = await ReadLineAsync(ns); // FILESIZE ...

            string fileName = "file.bin";
            long size = 0;

            if (fnLine != null && fnLine.StartsWith("FILENAME ", StringComparison.OrdinalIgnoreCase))
            {
                var b64 = fnLine.Substring(9).Trim();
                try
                {
                    fileName = Utf8NoBom.GetString(Convert.FromBase64String(b64));
                    if (string.IsNullOrWhiteSpace(fileName)) fileName = "file.bin";
                    fileName = Path.GetFileName(fileName); // sanitize
                }
                catch { fileName = "file.bin"; }
            }

            if (szLine != null && szLine.StartsWith("FILESIZE ", StringComparison.OrdinalIgnoreCase))
            {
                long.TryParse(szLine.Substring(9).Trim(), out size);
            }

            if (size < 0)
            {
                await WriteLineAsync(ns, "NO");
                c.Close();
                return;
            }

            Directory.CreateDirectory("downloads");
            var target = MakeUniquePath(Path.Combine("downloads", fileName));

            await WriteLineAsync(ns, "OK");

            Console.WriteLine($"[FILE] incoming from {senderName}: '{fileName}' ({size} bytes) -> {target}");

            using (var fs = File.Create(target))
            {
                await CopyExactAsync(ns, fs, size);
            }

            Console.WriteLine($"[FILE] received complete: {target}");
            c.Close();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FILE] receive failed ({ex.Message})");
            try { c.Close(); } catch { }
        }
    }

    static string MakeUniquePath(string path)
    {
        if (!File.Exists(path)) return path;

        var dir = Path.GetDirectoryName(path) ?? ".";
        var name = Path.GetFileNameWithoutExtension(path);
        var ext = Path.GetExtension(path);

        for (int i = 1; i < 10000; i++)
        {
            var p = Path.Combine(dir, $"{name}({i}){ext}");
            if (!File.Exists(p)) return p;
        }
        return Path.Combine(dir, $"{name}({Guid.NewGuid():N}){ext}");
    }

    static async Task CopyExactAsync(Stream input, Stream output, long bytes)
    {
        byte[] buf = new byte[64 * 1024];
        long remaining = bytes;

        while (remaining > 0)
        {
            int toRead = remaining > buf.Length ? buf.Length : (int)remaining;
            int n = await input.ReadAsync(buf, 0, toRead);
            if (n <= 0) throw new EndOfStreamException("unexpected EOF during file transfer");
            await output.WriteAsync(buf, 0, n);
            remaining -= n;
        }
        await output.FlushAsync();
    }

    // ================= RAW LINE IO (NetworkStream) =================

    static async Task<string?> ReadLineAsync(NetworkStream ns, int max = 16_384)
    {
        var ms = new MemoryStream();
        byte[] b = new byte[1];

        while (ms.Length < max)
        {
            int n = await ns.ReadAsync(b, 0, 1);
            if (n <= 0)
            {
                if (ms.Length == 0) return null;
                break;
            }

            if (b[0] == (byte)'\n')
                break;

            if (b[0] != (byte)'\r')
                ms.WriteByte(b[0]);
        }

        return Utf8NoBom.GetString(ms.ToArray());
    }

    static async Task WriteLineAsync(NetworkStream ns, string line)
    {
        var bytes = Utf8NoBom.GetBytes(line + "\n");
        await ns.WriteAsync(bytes, 0, bytes.Length);
        await ns.FlushAsync();
    }
}
