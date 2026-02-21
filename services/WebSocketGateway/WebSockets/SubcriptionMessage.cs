namespace WebSocketGateway.WebSockets;

public class SubscriptionMessage
{
    public string? Type { get; set; }
    public string[]? Symbols { get; set; }
}
