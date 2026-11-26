using RabbitMq_Aspnet.ConnectionProvider;
using RabbitMq_Aspnet.Options;
using RabbitMq_Aspnet.PublisherConsumer;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

builder.Services.Configure<RabbitMqOptions>(
    builder.Configuration.GetSection("RabbitMq"));
builder.Services.AddSingleton<IRabbitMqConnectionProvider,
    RabbitMqConnectionProvider>();
builder.Services.AddSingleton<IMessagePublisher, MessagePublisher>();
builder.Services.AddHostedService<OrderPlacedConsumer>();


var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var provider = scope.ServiceProvider.GetRequiredService<IRabbitMqConnectionProvider>();

    var conn = await provider.GetConnectionAsync();

    await using var ch = await conn.CreateChannelAsync();

    await Topology.DeclareAsync(ch);
}

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapPost("/orders", async (CreateOrderRequest req,
    IMessagePublisher publisher) =>
{
    var orderId = Guid.NewGuid();
    // 1. save the order to your DB (omitted)
    // 2. publish the event
    await publisher.PublishAsync(Topology.Exchange, Topology.RoutingKey,
        new OrderPlaced(orderId, req.email, req.total));
    return Results.Accepted($"/orders/{orderId}", new { orderId });
});


app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();

public record CreateOrderRequest(string email, decimal total);