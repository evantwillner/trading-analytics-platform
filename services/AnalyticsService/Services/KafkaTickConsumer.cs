using System.Text.Json;
using AnalyticsService.Models;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace AnalyticsService.Services;

/// <summary>
/// Background worker that consumes tick events from Kafka (Redpanda locally),
/// deserializes them from JSON, and updates the TickStore.
/// </summary>
public class KafkaTickConsumer : BackgroundService
{
    private readonly TickStore _tickStore;
    private readonly string _bootstrapServers;
    private readonly string _topic;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public KafkaTickConsumer(TickStore tickStore, IConfiguration configuration)
    {
        _tickStore = tickStore;
        _bootstrapServers = configuration["KafkaBootstrapServers"] ?? "localhost:9092";
        _topic = configuration["KafkaTopic"] ?? "ticks.v1";
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() =>
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = "analytics-service",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };

            using var consumer = new ConsumerBuilder<string, string>(config).Build();

            consumer.Subscribe(_topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Consume(stoppingToken);

                        if (result?.Message?.Value is null)
                            continue;

                        var tick = JsonSerializer.Deserialize<Tick>(result.Message.Value, JsonOptions);

                        if (tick is null || string.IsNullOrWhiteSpace(tick.Symbol))
                            continue;

                        _tickStore.Update(tick);
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"Kafka consume error: {ex.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // expected on shutdown
            }
            finally
            {
                consumer.Close();
            }
        }, stoppingToken);
    }
}