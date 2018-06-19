using Orleans.Providers.Streams.Common;
using Orleans.Streams.BatchContainer;

namespace Orleans.Streams
{
    public class RabbitMqStreamProviderIntegration<TSerializer> : PersistentStreamProvider<RabbitMqAdapterFactory<TSerializer>> where TSerializer : IBatchContainerSerializer, new()
    {
    }
}
