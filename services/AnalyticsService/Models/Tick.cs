namespace AnalyticsService.Models;

public class Tick
{
    public string Type { get; set; } = default!;
    public string Symbol { get; set; } = default!;
    public long TsUnixMs { get; set; }
    public double Price { get; set; }
    public long Size { get; set; }
}