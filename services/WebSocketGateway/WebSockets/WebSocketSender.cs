using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace WebSocketGateway.WebSockets;

public static class WebSocketSender
{
    public static async Task SendJsonAsync(WebSocket socket, object payload, CancellationToken ct)
    {
        var json = JsonSerializer.Serialize(payload);
        var bytes = Encoding.UTF8.GetBytes(json);

        await socket.SendAsync(
            bytes,
            WebSocketMessageType.Text,
            endOfMessage: true,
            cancellationToken: ct
        );
    }
}