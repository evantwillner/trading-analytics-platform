using System.Collections.Concurrent;
using System.Net.WebSockets;

namespace WebSocketGateway.WebSockets;

/// <summary>
/// Tracks connected WebSocket clients and their per-client subscription state.
/// </summary>
public class ClientConnectionManager
{
    // ClientId -> WebSocket
    private readonly ConcurrentDictionary<string, WebSocket> _clients = new();

    // ClientId -> subscribed symbols set
    // represent a set as a ConcurrentDictionary(symbol -> true) for thread-safety.
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _subscriptions = new();

    public string Add(WebSocket socket)
    {
        var id = Guid.NewGuid().ToString("N");
        _clients[id] = socket;
        _subscriptions[id] = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);
        return id;
    }

    public void Remove(string clientId)
    {
        _clients.TryRemove(clientId, out _);
        _subscriptions.TryRemove(clientId, out _);
    }

    public IReadOnlyDictionary<string, WebSocket> ClientsSnapshot() => _clients;

    public bool IsSubscribed(string clientId, string symbol)
    {
        return _subscriptions.TryGetValue(clientId, out var set) && set.ContainsKey(symbol);
    }

    public void Subscribe(string clientId, IEnumerable<string> symbols)
    {
        if (!_subscriptions.TryGetValue(clientId, out var set)) return;

        foreach (var s in symbols)
        {
            var symbol = s?.Trim();
            if (string.IsNullOrWhiteSpace(symbol)) continue;
            set[symbol] = 1;
        }
    }

    public void Unsubscribe(string clientId, IEnumerable<string> symbols)
    {
        if (!_subscriptions.TryGetValue(clientId, out var set)) return;

        foreach (var s in symbols)
        {
            var symbol = s?.Trim();
            if (string.IsNullOrWhiteSpace(symbol)) continue;
            set.TryRemove(symbol, out _);
        }
    }

    public void SetSymbols(string clientId, IEnumerable<string> symbols)
    {
        if (!_subscriptions.TryGetValue(clientId, out var set)) return;

        set.Clear();
        Subscribe(clientId, symbols);
    }
}
