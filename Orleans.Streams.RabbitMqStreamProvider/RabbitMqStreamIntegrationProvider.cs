using Orleans.Providers.Streams.Common;
using Orleans.Streams.BatchContainer;

namespace Orleans.Streams
{
    public abstract class RabbitMqStreamIntegrationProvider<TSerializer> : PersistentStreamProvider<RabbitMqAdapterFactory<TSerializer>> where TSerializer : IBatchContainerSerializer, new()
    {
    }
}
