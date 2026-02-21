using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace WebSocketGateway.WebSockets;

public class MarketDataBroadcaster
{
    private readonly ClientConnectionManager _connections;

    public MarketDataBroadcaster(ClientConnectionManager connections)
    {
        _connections = connections;
    }

    public async Task BroadcastTickAsync(string symbol, object payload, CancellationToken ct)
    {
        var json = JsonSerializer.Serialize(payload);
        var bytes = Encoding.UTF8.GetBytes(json);

        var clients = _connections.ClientsSnapshot();

        foreach (var kvp in clients)
        {
            var clientId = kvp.Key;
            var socket = kvp.Value;

            if (socket.State != WebSocketState.Open)
                continue;

            if (!_connections.IsSubscribed(clientId, symbol))
                continue;

            try
            {
                await socket.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
            }
            catch
            {
                _connections.Remove(clientId);
            }
        }
    }
}
