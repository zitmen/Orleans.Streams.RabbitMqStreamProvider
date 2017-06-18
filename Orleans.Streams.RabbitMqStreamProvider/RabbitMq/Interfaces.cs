using System;
using Orleans.Runtime;
using RabbitMQ.Client;

namespace Orleans.Streams.RabbitMq
{
    internal interface IRabbitMqConnectorFactory
    {
        Logger Logger { get; }
        IRabbitMqConsumer CreateConsumer(QueueId queueId);
        IRabbitMqProducer CreateProducer(QueueId queueId);
    }

    internal interface IRabbitMqConsumer : IDisposable
    {
        void Ack(ulong deliveryTag);
        void Nack(ulong deliveryTag);
        BasicGetResult Receive();
    }

    internal interface IRabbitMqProducer : IDisposable
    {
        void Send(byte[] message);
    }
}
