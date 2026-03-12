using System.Collections.Concurrent;
using System.Net.WebSockets;

namespace WebSocketGateway.WebSockets;

public class ClientConnectionManager
{
    private readonly ConcurrentDictionary<string, ClientSession> _sessions = new();

    public ClientSession Add(WebSocket socket)
    {
        var id = Guid.NewGuid().ToString("N");
        var session = new ClientSession(id, socket);
        _sessions[id] = session;
        return session;
    }

    public bool TryGet(string clientId, out ClientSession session)
        => _sessions.TryGetValue(clientId, out session!);

    public IReadOnlyDictionary<string, ClientSession> SessionsSnapshot() => _sessions;

    public void Remove(string clientId)
    {
        if (_sessions.TryRemove(clientId, out var session))
        {
            try { session.GrpcCts?.Cancel(); } catch { }
            try { session.GrpcCts?.Dispose(); } catch { }
        }
    }
}