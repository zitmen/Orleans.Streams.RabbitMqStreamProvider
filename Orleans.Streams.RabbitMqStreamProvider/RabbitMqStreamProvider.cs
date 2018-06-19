using Orleans.Providers.Streams.Common;
using Orleans.Streams.BatchContainer;

namespace Orleans.Streams
{
    public class RabbitMqStreamProvider : PersistentStreamProvider<RabbitMqAdapterFactory<DefaultBatchContainerSerializer>>
    {
    }
}