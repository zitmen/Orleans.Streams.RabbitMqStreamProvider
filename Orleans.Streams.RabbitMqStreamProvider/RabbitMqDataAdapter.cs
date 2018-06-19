using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Streams.BatchContainer;

namespace Orleans.Streams
{
    public static class RabbitMqDataAdapter
    {
        public static RabbitMqBatchContainer FromQueueMessage(IBatchContainerSerializer serializer, byte[] data, long sequenceId, ulong deliveryTag)
        {
            var batchContainer = serializer.Deserialize(data);
            batchContainer.EventSequenceToken = new EventSequenceToken(sequenceId);
            batchContainer.DeliveryTag = deliveryTag;
            return batchContainer;
        }

        public static byte[] ToQueueMessage<T>(IBatchContainerSerializer serializer, Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            return serializer.Serialize(new RabbitMqBatchContainer(streamGuid, streamNamespace, events.Cast<object>().ToList(), requestContext));
        }
    }
}