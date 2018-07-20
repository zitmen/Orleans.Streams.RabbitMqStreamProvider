using Microsoft.Extensions.Logging;
using Orleans.Configuration;

namespace Orleans.Streams.RabbitMq
{
    internal class RabbitMqOnlineConnectorFactory : IRabbitMqConnectorFactory
    {
        private readonly RabbitMqOptions _options;
        
        public RabbitMqOnlineConnectorFactory(RabbitMqOptions options, ILoggerFactory loggerFactory)
        {
            _options = options;
            LoggerFactory = loggerFactory;
        }

        public ILoggerFactory LoggerFactory { get; }

        public IRabbitMqConsumer CreateConsumer(QueueId queueId)
            => new RabbitMqConsumer(new RabbitMqConnector(_options, queueId, LoggerFactory.CreateLogger($"{typeof(RabbitMqConsumer).FullName}.{queueId}")));

        public IRabbitMqProducer CreateProducer(QueueId queueId)
            => new RabbitMqProducer(new RabbitMqConnector(_options, queueId, LoggerFactory.CreateLogger($"{typeof(RabbitMqProducer).FullName}.{queueId}")));
    }
}