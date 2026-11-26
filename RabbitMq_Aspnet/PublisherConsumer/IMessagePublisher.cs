using RabbitMQ.Client;
using RabbitMq_Aspnet.ConnectionProvider;
using System.Text.Json;

namespace RabbitMq_Aspnet.PublisherConsumer
{
    public interface IMessagePublisher
    {
        Task PublishAsync<T>(string exchange, string rounteKey, T message, CancellationToken ct = default);
    }


    public class MessagePublisher : IMessagePublisher
    {
        private readonly IRabbitMqConnectionProvider _provider;
        public MessagePublisher(IRabbitMqConnectionProvider provider)
        {
            _provider = provider;
        }
        public async Task PublishAsync<T>(string exchange, string rounteKey, T message, CancellationToken ct = default)
        {
            var options = new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
    publisherConfirmationTrackingEnabled: true);

            var connection = await _provider.GetConnectionAsync(ct);

            await using var channel = await connection.CreateChannelAsync(options, ct);

            var props = new BasicProperties
            {
                Persistent = true,
                MessageId = Guid.NewGuid().ToString(),
                ContentType = "application/json",
                Type = typeof(T).Name,
                Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
            };

            var body = JsonSerializer.SerializeToUtf8Bytes(message);

            await channel.BasicPublishAsync(exchange, routingKey: rounteKey,
                mandatory: true, basicProperties: props,
                body: body, cancellationToken: ct);

        }
    }
}
