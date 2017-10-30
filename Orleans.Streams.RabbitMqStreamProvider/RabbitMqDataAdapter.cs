using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;

namespace Orleans.Streams
{
    public static class RabbitMqDataAdapter
    {
        public static RabbitMqBatchContainer FromQueueMessage(SerializationManager serializationManager, byte[] data, long sequenceId, ulong deliveryTag)
        {
            var batchContainer = serializationManager.DeserializeFromByteArray<RabbitMqBatchContainer>(data);
            batchContainer.EventSequenceToken = new EventSequenceToken(sequenceId);
            batchContainer.DeliveryTag = deliveryTag;
            return batchContainer;
        }

        public static byte[] ToQueueMessage<T>(SerializationManager serializationManager, Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            return serializationManager.SerializeToByteArray(new RabbitMqBatchContainer(streamGuid, streamNamespace, events.Cast<object>().ToList(), requestContext));
        }
    }
}