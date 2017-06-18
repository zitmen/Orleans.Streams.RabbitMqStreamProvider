using Orleans.Runtime;

namespace Orleans.Streams.RabbitMq
{
    internal class RabbitMqOnlineConnectorFactory : IRabbitMqConnectorFactory
    {
        private readonly RabbitMqStreamProviderOptions _options;
        
        public RabbitMqOnlineConnectorFactory(RabbitMqStreamProviderOptions options, Logger logger)
        {
            _options = options;
            Logger = logger;
        }

        public Logger Logger { get; }

        public IRabbitMqConsumer CreateConsumer(QueueId queueId)
            => new RabbitMqConsumer(new RabbitMqConnector(_options, queueId, Logger));

        public IRabbitMqProducer CreateProducer(QueueId queueId)
            => new RabbitMqProducer(new RabbitMqConnector(_options, queueId, Logger));
    }
}