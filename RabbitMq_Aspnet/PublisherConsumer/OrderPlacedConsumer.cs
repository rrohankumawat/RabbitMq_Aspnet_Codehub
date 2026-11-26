using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMq_Aspnet.ConnectionProvider;
using System.Text.Json;

namespace RabbitMq_Aspnet.PublisherConsumer
{
    public record OrderPlaced(Guid orderid, string email, decimal totolprice);
    public class OrderPlacedConsumer : BackgroundService
    {
        private readonly IRabbitMqConnectionProvider _provider;
        private IChannel? _channel;
        public OrderPlacedConsumer(IRabbitMqConnectionProvider provider)
        {
            _provider = provider;
        }
        protected override async Task ExecuteAsync(CancellationToken ct)
        {
            var conn = await _provider.GetConnectionAsync(ct);
            _channel = await conn.CreateChannelAsync(cancellationToken: ct);
            await Topology.DeclareAsync(_channel, ct);
            await _channel.BasicQosAsync(0, 10, false, ct);

            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.ReceivedAsync += async (_, ea) =>
            {
                try
                {
                    var m = JsonSerializer.Deserialize<OrderPlaced>(ea.Body.Span);  // JSON to OrderPlaced
                    Console.WriteLine($"Consumer: {m.orderid}, {m.email}, {m.totolprice}");                                 // ... real work ...
                    await _channel.BasicAckAsync(ea.DeliveryTag, false);
                }
                catch
                {
                    await _channel.BasicNackAsync(ea.DeliveryTag, false, false);
                }
            };
            await _channel.BasicConsumeAsync(Topology.EmailQueue, false, consumer);

            await Task.Delay(Timeout.Infinite, ct).ContinueWith(_ => { });
        }


        public override async Task StopAsync(CancellationToken ct)
        {
            if (_channel is not null) await _channel.CloseAsync(ct);
            await base.StopAsync(ct);
        }
    }
}
