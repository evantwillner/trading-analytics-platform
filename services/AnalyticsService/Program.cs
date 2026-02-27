using AnalyticsService.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddGrpc();
builder.Services.AddSingleton<TickStore>();
builder.Services.AddHostedService<KafkaTickConsumer>();

var app = builder.Build();


// Configure the HTTP request pipeline. When gRPC traffic arrives for this contract, route it to the AnalyticsGrpcService implementation.
app.MapGrpcService<AnalyticsGrpcService>();
app.MapGet("/", () => "AnalyticsService is running. Use a gRPC client to connect.");
app.Run();
