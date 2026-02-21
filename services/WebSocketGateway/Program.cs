using Grpc.Net.Client;
using TradingAnalytics.Proto.Analytics.V1;
using WebSocketGateway.Streaming;
using WebSocketGateway.WebSockets;
using System.Text;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// --- Config: where AnalyticsService is running ---
// IMPORTANT: Use http:// for h2c (plaintext) since you tested grpcurl with -plaintext.
var analyticsGrpcAddress = builder.Configuration["AnalyticsGrpcAddress"] ?? "http://localhost:5226";

// --- DI registrations ---
builder.Services.AddSingleton(new ClientConnectionManager());
builder.Services.AddSingleton<MarketDataBroadcaster>();

builder.Services.AddSingleton(sp =>
{
    var channel = GrpcChannel.ForAddress(analyticsGrpcAddress);
    return new AnalyticsService.AnalyticsServiceClient(channel);
});

builder.Services.AddHostedService<BridgeHostedService>();

var app = builder.Build();

app.UseWebSockets();

app.MapGet("/", () => "WebSocketGateway is running");

// WebSocket endpoint for market data
app.Map("/ws/marketdata", async context =>
{
    var connections = context.RequestServices.GetRequiredService<ClientConnectionManager>();

    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        return;
    }

    using var socket = await context.WebSockets.AcceptWebSocketAsync();
    var clientId = connections.Add(socket);
    Console.WriteLine($"WS client connected: {clientId}");

    var buffer = new byte[1024 * 8];

    while (socket.State == System.Net.WebSockets.WebSocketState.Open)
    {
        var result = await socket.ReceiveAsync(buffer, CancellationToken.None);

        if (result.MessageType == System.Net.WebSockets.WebSocketMessageType.Close)
            break;

        if (result.MessageType != System.Net.WebSockets.WebSocketMessageType.Text)
            continue;

        var json = Encoding.UTF8.GetString(buffer, 0, result.Count);

        SubscriptionMessage? msg = null;
        try
        {
            msg = JsonSerializer.Deserialize<SubscriptionMessage>(json);
        }
        catch
        {
            // Ignore invalid JSON for now.
            continue;
        }

        if (msg?.Type is null || msg.Symbols is null)
            continue;

        switch (msg.Type.Trim().ToLowerInvariant())
        {
            case "subscribe":
                connections.Subscribe(clientId, msg.Symbols);
                break;

            case "unsubscribe":
                connections.Unsubscribe(clientId, msg.Symbols);
                break;

            case "set_symbols":
                connections.SetSymbols(clientId, msg.Symbols);
                break;

            default:
                // unknown message type; ignore
                break;
        }
    }


    connections.Remove(clientId);
    Console.WriteLine($"WS client disconnected: {clientId}");
});

app.Run();


// Hosted service wrapper that runs the bridge in the background
public class BridgeHostedService : BackgroundService
{
    private readonly IServiceProvider _sp;

    public BridgeHostedService(IServiceProvider sp)
    {
        _sp = sp;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Resolve bridge dependencies from DI
        using var scope = _sp.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<AnalyticsService.AnalyticsServiceClient>();
        var broadcaster = scope.ServiceProvider.GetRequiredService<MarketDataBroadcaster>();

        var bridge = new AnalyticsStreamBridge(client, broadcaster);

        Console.WriteLine("Starting Analytics ➜ WS bridge...");
        await bridge.RunAsync(stoppingToken);
    }
}
