using System.Threading.Channels;
using TradingAnalytics.Proto.Analytics.V1;
using WebSocketGateway.WebSockets;

namespace WebSocketGateway.Streaming;

public class GrpcToWebSocketPump
{
    private readonly AnalyticsService.AnalyticsServiceClient _grpcClient;

    public GrpcToWebSocketPump(AnalyticsService.AnalyticsServiceClient grpcClient)
    {
        _grpcClient = grpcClient;
    }

    public Task StartOrRestartAsync(ClientSession session, CancellationToken serverCt)
    {
        // Cancel prior stream if it exists
        session.GrpcCts?.Cancel();
        session.GrpcCts?.Dispose();

        if (session.Symbols.Count == 0)
        {
            Console.WriteLine($"No symbols for client {session.ClientId}; not starting gRPC stream.");
            return Task.CompletedTask;
        }

        session.GrpcCts = CancellationTokenSource.CreateLinkedTokenSource(serverCt);
        var ct = session.GrpcCts.Token;

        // Channel gives us a single-reader/single-writer style handoff
        var channel = Channel.CreateUnbounded<TradingAnalytics.Proto.Analytics.V1.Tick>();

        // Task A: Read from gRPC stream and push into channel
        _ = Task.Run(async () =>
        {
            try
            {
                var request = new StreamTicksRequest();
                request.Symbols.AddRange(session.Symbols);

                Console.WriteLine($"Starting gRPC stream for client {session.ClientId} with symbols: {string.Join(",", session.Symbols)}");

                using var call = _grpcClient.StreamTicks(request, cancellationToken: ct);

                while (await call.ResponseStream.MoveNext(ct))
                {
                    
                    var tick = call.ResponseStream.Current;
                    await channel.Writer.WriteAsync(tick, ct);

                    Console.WriteLine($"Received tick from gRPC for client {session.ClientId}: {tick.Symbol} {tick.Price}");
                }
            }
            catch (OperationCanceledException)
            {
                // expected when restarting/changing subscriptions or disconnecting
            }
            catch (Exception ex)
            {
                Console.WriteLine($"gRPC stream error for {session.ClientId}: {ex.Message}");
            }
            finally
            {
                channel.Writer.TryComplete();
            }
        }, ct);

        // Task B: Read from channel and write to WebSocket
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var tick in channel.Reader.ReadAllAsync(ct))
                {
                    if (session.Socket.State != System.Net.WebSockets.WebSocketState.Open)
                        break;

                    var payload = new
                    {
                        type = "tick",
                        symbol = tick.Symbol,
                        tsUnixMs = tick.TsUnixMs,
                        price = tick.Price,
                        size = tick.Size
                    };

                    Console.WriteLine($"Sending tick to WS client {session.ClientId}: {tick.Symbol} {tick.Price} {tick.Size}");
                    await WebSocketSender.SendJsonAsync(session.Socket, payload, ct);
                }
            }
            catch (OperationCanceledException)
            {
                // expected on restart/disconnect
            }
            catch (Exception ex)
            {
                Console.WriteLine($"WebSocket send error for {session.ClientId}: {ex.Message}");
            }
        }, ct);

        return Task.CompletedTask;
    }
}