using AnalyticsService.Models;
using AnalyticsService.Services;
using Grpc.Core;
using TradingAnalytics.Proto.Analytics.V1;

namespace AnalyticsService.Services;

public class AnalyticsGrpcService : TradingAnalytics.Proto.Analytics.V1.AnalyticsService.AnalyticsServiceBase
{
    private readonly TickStore _tickStore;

    public AnalyticsGrpcService(TickStore tickStore)
    {
        _tickStore = tickStore;
    }

    public override Task<GetSnapshotResponse> GetSnapshot(GetSnapshotRequest request, ServerCallContext context)
    {
        var latest = _tickStore.GetSnapshot(request.Symbol);

        if (latest is null)
        {
            // Return an empty response if we have no tick yet
            return Task.FromResult(new GetSnapshotResponse());
        }

        var protoTick = new TradingAnalytics.Proto.Analytics.V1.Tick
        {
            Symbol = latest.Symbol,
            TsUnixMs = latest.TsUnixMs,
            Price = latest.Price,
            Size = latest.Size
        };

        return Task.FromResult(new GetSnapshotResponse { LastTick = protoTick });
    }

    public override async Task StreamTicks(
        StreamTicksRequest request,
        IServerStreamWriter<TradingAnalytics.Proto.Analytics.V1.Tick> responseStream,
        ServerCallContext context)
    {
        var ct = context.CancellationToken;

        // If client didn't specify symbols, stream everything we receive.
        var symbols = request.Symbols?.ToHashSet(StringComparer.OrdinalIgnoreCase);

        // Need to write ticks to the gRPC stream whenever TickStore receives one
        // TickReceived is an event;  handler will push updates to this client
        void Handler(AnalyticsService.Models.Tick t)
        {
            if (symbols is not null && symbols.Count > 0 && !symbols.Contains(t.Symbol))
                return;

            // Fire-and-forget, but observe cancellation via ct in WriteAsync call.
            _ = responseStream.WriteAsync(new TradingAnalytics.Proto.Analytics.V1.Tick
            {
                Symbol = t.Symbol,
                TsUnixMs = t.TsUnixMs,
                Price = t.Price,
                Size = t.Size
            });
        }

        _tickStore.TickReceived += Handler;

        try
        {
            // Keep the RPC open until the client disconnects.
            await Task.Delay(Timeout.Infinite, ct);
        }
        catch (OperationCanceledException)
        {
            // expected when client disconnects
        }
        finally
        {
            _tickStore.TickReceived -= Handler;
        }
    }
}