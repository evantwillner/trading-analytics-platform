using System.Net.WebSockets;

namespace WebSocketGateway.WebSockets;

public sealed class ClientSession
{
    public string ClientId { get; }
    public WebSocket Socket { get; }
    public HashSet<string> Symbols { get; } = new(StringComparer.OrdinalIgnoreCase);

    // Controls the lifetime of the gRPC streaming call for this client
    public CancellationTokenSource? GrpcCts { get; set; }

    public ClientSession(string clientId, WebSocket socket)
    {
        ClientId = clientId;
        Socket = socket;
    }
}