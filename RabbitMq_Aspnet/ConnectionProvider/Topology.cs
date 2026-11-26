using RabbitMQ.Client;

namespace RabbitMq_Aspnet.ConnectionProvider
{
    public static class Topology
    {
        public const string Exchange = "orders.events";
        public const string EmailQueue = "email.order-placed";
        public const string RoutingKey = "order.placed";
        // DeclareAsync: topic exchange + durable queue + binding

        public static async Task DeclareAsync(IChannel channel, CancellationToken ct = default)
        {
            await channel.ExchangeDeclareAsync(exchange: Exchange, ExchangeType.Topic,
                durable: true, cancellationToken: ct);

            await channel.QueueDeclareAsync(queue: EmailQueue, durable: true, exclusive: false,
                autoDelete: false, cancellationToken: ct);

            await channel.QueueBindAsync(queue: EmailQueue, 
                exchange: Exchange, routingKey: RoutingKey, cancellationToken: ct);
        }
    }




}
