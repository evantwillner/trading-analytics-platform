using Grpc.Core;
using TradingAnalytics.Proto.Analytics.V1;
using WebSocketGateway.WebSockets;

namespace WebSocketGateway.Streaming;

public class AnalyticsStreamBridge
{
    private readonly AnalyticsService.AnalyticsServiceClient _client;
    private readonly MarketDataBroadcaster _broadcaster;

    public AnalyticsStreamBridge(
        AnalyticsService.AnalyticsServiceClient client,
        MarketDataBroadcaster broadcaster)
    {
        _client = client;
        _broadcaster = broadcaster;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        // For now: fixed symbols to prove the bridge works end-to-end.
        // Next step: per-client subscribe/unsubscribe.
        var request = new StreamTicksRequest();
        request.Symbols.AddRange(new[] { "AAPL", "MSFT", "TSLA" });

        using var call = _client.StreamTicks(request, cancellationToken: ct);

        await foreach (var tick in call.ResponseStream.ReadAllAsync(ct))
        {
            // Convert to a JSON-friendly shape
            var payload = new
            {
                type = "tick",
                symbol = tick.Symbol,
                tsUnixMs = tick.TsUnixMs,
                price = tick.Price,
                size = tick.Size
            };

            await _broadcaster.BroadcastTickAsync(tick.Symbol, payload, ct);
        }
    }
}
