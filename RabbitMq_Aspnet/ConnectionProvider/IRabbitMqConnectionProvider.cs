using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMq_Aspnet.Options;
using System.Data.Common;

namespace RabbitMq_Aspnet.ConnectionProvider
{
    public interface IRabbitMqConnectionProvider
    {
        Task<IConnection> GetConnectionAsync(CancellationToken ct = default);
    }


    public class RabbitMqConnectionProvider : IRabbitMqConnectionProvider
    {
        private readonly IConnectionFactory _connectionFactory;
        private IConnection? _connection;
        private readonly SemaphoreSlim _lock = new(1, 1);
        public RabbitMqConnectionProvider(
            IOptions<RabbitMqOptions> options)
        {
            var o = options.Value;

            _connectionFactory = new ConnectionFactory
            {
                HostName = o.HostName,
                UserName = o.Username,
                Password = o.Password,
                Port = o.Port,
                VirtualHost = o.VirtualHost,
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
                ClientProvidedName = "order-service"
            };
        }
        public async Task<IConnection> GetConnectionAsync(CancellationToken ct = default)
        {
            if (_connection is { IsOpen: true })
                return _connection;

            await _lock.WaitAsync(ct);   
            try
            {
                if (_connection is not { IsOpen: true })
                    _connection =await _connectionFactory.CreateConnectionAsync(ct);
                return _connection;
            }
            finally { _lock.Release(); }
        }

    }
}
