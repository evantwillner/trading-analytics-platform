using System.Text;
using System.Text.Json;
using Grpc.Net.Client;
using TradingAnalytics.Proto.Analytics.V1;
using WebSocketGateway.Streaming;
using WebSocketGateway.WebSockets;

var builder = WebApplication.CreateBuilder(args);

// Where AnalyticsService is running.
// Use http:// because your local gRPC service is running plaintext/h2c on localhost:5226.
var analyticsGrpcAddress = builder.Configuration["AnalyticsGrpcAddress"] ?? "http://localhost:5226";

// Dependency Injection registrations
builder.Services.AddSingleton<ClientConnectionManager>();
builder.Services.AddSingleton<GrpcToWebSocketPump>();

builder.Services.AddSingleton(sp =>
{
    var channel = GrpcChannel.ForAddress(analyticsGrpcAddress);
    return new AnalyticsService.AnalyticsServiceClient(channel);
});

var app = builder.Build();

app.UseWebSockets();

app.MapGet("/", () => "WebSocketGateway is running");

app.Map("/ws/marketdata", async context =>
{
    var connections = context.RequestServices.GetRequiredService<ClientConnectionManager>();
    var pump = context.RequestServices.GetRequiredService<GrpcToWebSocketPump>();

    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        return;
    }

    using var socket = await context.WebSockets.AcceptWebSocketAsync();
    var session = connections.Add(socket);

    Console.WriteLine($"WS client connected: {session.ClientId}");

    var buffer = new byte[1024 * 8];

    try
    {
        while (socket.State == System.Net.WebSockets.WebSocketState.Open)
        {
            var result = await socket.ReceiveAsync(buffer, CancellationToken.None);

            if (result.MessageType == System.Net.WebSockets.WebSocketMessageType.Close)
                break;

            if (result.MessageType != System.Net.WebSockets.WebSocketMessageType.Text)
                continue;

            var json = Encoding.UTF8.GetString(buffer, 0, result.Count);

            SubscriptionMessage? msg;
            try
            {
                msg = JsonSerializer.Deserialize<SubscriptionMessage>(json);
            }
            catch
            {
                // Ignore malformed JSON for now
                continue;
            }

            if (msg?.Type is null || msg.Symbols is null)
                continue;

            var type = msg.Type.Trim().ToLowerInvariant();

            if (type == "subscribe")
            {
                foreach (var s in msg.Symbols)
                {
                    var sym = s?.Trim();
                    if (!string.IsNullOrWhiteSpace(sym))
                        session.Symbols.Add(sym);
                }

                await pump.StartOrRestartAsync(session, context.RequestAborted);
            }
            else if (type == "unsubscribe")
            {
                foreach (var s in msg.Symbols)
                {
                    var sym = s?.Trim();
                    if (!string.IsNullOrWhiteSpace(sym))
                        session.Symbols.Remove(sym);
                }

                await pump.StartOrRestartAsync(session, context.RequestAborted);
            }
            else if (type == "set_symbols")
            {
                session.Symbols.Clear();

                foreach (var s in msg.Symbols)
                {
                    var sym = s?.Trim();
                    if (!string.IsNullOrWhiteSpace(sym))
                        session.Symbols.Add(sym);
                }

                await pump.StartOrRestartAsync(session, context.RequestAborted);
            }
        }
    }
    finally
    {
        connections.Remove(session.ClientId);
        Console.WriteLine($"WS client disconnected: {session.ClientId}");
    }
});

app.Run();