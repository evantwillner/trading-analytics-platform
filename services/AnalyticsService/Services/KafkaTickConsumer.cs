using System.Text.Json;
using AnalyticsService.Models;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

namespace AnalyticsService.Services;

/// <summary>
/// Background worker that consumes tick events from Kafka (Redpanda locally),
/// deserializes them from JSON, and updates the TickStore.
/// </summary>
public class KafkaTickConsumer : BackgroundService
{
    private readonly TickStore _tickStore;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public KafkaTickConsumer(TickStore tickStore)
    {
        _tickStore = tickStore;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Run the consumer loop
        return Task.Run(() =>
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "analytics-service",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };

            using var consumer = new ConsumerBuilder<string, string>(config).Build();

            consumer.Subscribe("ticks.v1");

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        // Blocks until a message arrives or cancellation occurs
                        var result = consumer.Consume(stoppingToken);

                        if (result?.Message?.Value is null)
                            continue;

                        var tick = JsonSerializer.Deserialize<Tick>(result.Message.Value, JsonOptions);

                        // Guard malformed/unknown schema
                        if (tick is null || string.IsNullOrWhiteSpace(tick.Symbol))
                            continue;

                        _tickStore.Update(tick);
                    }
                    catch (ConsumeException ex)
                    {
                        // Kafka-level error for a consume operation
                        Console.WriteLine($"Kafka consume error: {ex.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
               
            }
            finally
            {
                consumer.Close();
            }
        }, stoppingToken);
    }
}