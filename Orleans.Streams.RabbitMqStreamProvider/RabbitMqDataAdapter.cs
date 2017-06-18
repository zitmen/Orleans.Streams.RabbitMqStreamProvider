using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;

namespace Orleans.Streams
{
    public static class RabbitMqDataAdapter
    {
        public static RabbitMqBatchContainer FromQueueMessage(byte[] data, long sequenceId, ulong deliveryTag)
        {
            var batchContainer = SerializationManager.DeserializeFromByteArray<RabbitMqBatchContainer>(data);
            batchContainer.EventSequenceToken = new EventSequenceToken(sequenceId);
            batchContainer.DeliveryTag = deliveryTag;
            return batchContainer;
        }

        public static byte[] ToQueueMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            var batchContainer = new RabbitMqBatchContainer(streamGuid, streamNamespace, events.Cast<object>().ToList(), requestContext);
            return SerializationManager.SerializeToByteArray(batchContainer);
        }
    }
}