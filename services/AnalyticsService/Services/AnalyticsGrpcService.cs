using Grpc.Core;
using TradingAnalytics.Proto.Analytics.V1;

namespace AnalyticsService.Services;

public class AnalyticsGrpcService : TradingAnalytics.Proto.Analytics.V1.AnalyticsService.AnalyticsServiceBase
{
    private static readonly Random Rng = new();

    public override Task<GetSnapshotResponse> GetSnapshot(GetSnapshotRequest request, ServerCallContext context)
    {
        // Fake a "last tick" snapshot for now.
        var tick = new Tick
        {
            Symbol = request.Symbol,
            TsUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Price = 100 + Rng.NextDouble() * 10,
            Size = Rng.Next(1, 500)
        };

        return Task.FromResult(new GetSnapshotResponse { LastTick = tick });
    }

    public override async Task StreamTicks(
        StreamTicksRequest request,
        IServerStreamWriter<Tick> responseStream,
        ServerCallContext context)
    {
        // This token flips when the client disconnects in order to stop infinite loops safely
        var ct = context.CancellationToken;

        var symbols = request.Symbols.Count > 0
            ? request.Symbols.ToArray()
            : new[] { "AAPL", "MSFT", "TSLA" };

        while (!ct.IsCancellationRequested)
        {
            var symbol = symbols[Rng.Next(symbols.Length)];

            var tick = new Tick
            {
                Symbol = symbol,
                TsUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Price = 100 + Rng.NextDouble() * 10,
                Size = Rng.Next(1, 500)
            };

            await responseStream.WriteAsync(tick);

            await Task.Delay(50, ct);
        }
    }
}
