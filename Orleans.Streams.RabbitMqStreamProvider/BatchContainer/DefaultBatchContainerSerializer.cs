using System;
using Orleans.Serialization;

namespace Orleans.Streams.BatchContainer
{
    public class DefaultBatchContainerSerializer : IBatchContainerSerializer
    {
        private readonly SerializationManager _serializatonManager;

        public DefaultBatchContainerSerializer()
        {
            throw new NotImplementedException("Call `DefaultBatchContainerSerializer(SerializationManager)` instead!");
        }

        public DefaultBatchContainerSerializer(SerializationManager serializationManager)
        {
            _serializatonManager = serializationManager;
        }

        public RabbitMqBatchContainer Deserialize(byte[] data)
            => _serializatonManager.DeserializeFromByteArray<RabbitMqBatchContainer>(data);

        public byte[] Serialize(RabbitMqBatchContainer container)
            => _serializatonManager.SerializeToByteArray(container);
    }
}