using Orleans.Streams.BatchContainer;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace RabbitMqStreamTests
{
    public class ProtoBufBatchContainerSerializer : IBatchContainerSerializer
    {
        public byte[] Serialize(RabbitMqBatchContainer container)
        {
            using (var ms = new MemoryStream())
            {
                Serializer.Serialize(ms, container.GetEvents<Message>().Single().Item1);
                return ms.ToArray();
            }
        }

        public RabbitMqBatchContainer Deserialize(byte[] data)
        {
            using (var ms = new MemoryStream(data))
            {
                var notification = Serializer.Deserialize<Message>(ms);
                return new RabbitMqBatchContainer(
                    Guid.NewGuid(),
                    Globals.StreamNameSpace,
                    new List<object> { notification },
                    new Dictionary<string, object>());
            }
        }
    }
}
