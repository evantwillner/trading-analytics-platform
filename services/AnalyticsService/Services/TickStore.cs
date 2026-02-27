using System.Collections.Concurrent;
using AnalyticsService.Models;

namespace AnalyticsService.Services;

/// <summary>
/// In-memory store of the latest tick per symbol, plus an event for streaming consumers.
/// Thread-safe because Kafka consumption and gRPC requests happen concurrently.
/// </summary>
public class TickStore
{
    private readonly ConcurrentDictionary<string, Tick> _latestBySymbol =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Fired whenever a new tick is accepted into the store.
    /// gRPC streaming can subscribe to this event.
    /// </summary>
    public event Action<Tick>? TickReceived;

    public void Update(Tick tick)
    {
        // Harden against malformed messages (e.g., missing symbol)
        if (tick is null || string.IsNullOrWhiteSpace(tick.Symbol))
            return;

        _latestBySymbol[tick.Symbol] = tick;
        TickReceived?.Invoke(tick);
    }

    public Tick? GetSnapshot(string symbol)
    {
        if (string.IsNullOrWhiteSpace(symbol))
            return null;

        _latestBySymbol.TryGetValue(symbol, out var tick);
        return tick;
    }
}