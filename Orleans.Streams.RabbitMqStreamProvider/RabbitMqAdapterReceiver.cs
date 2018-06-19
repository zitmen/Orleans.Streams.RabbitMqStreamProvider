using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Streams.BatchContainer;
using Orleans.Streams.RabbitMq;

namespace Orleans.Streams
{
    internal class RabbitMqAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly IRabbitMqConnectorFactory _rmqConnectorFactory;
        private readonly QueueId _queueId;
        private readonly IBatchContainerSerializer _serializer;
        private readonly TimeSpan _cacheFillingTimeout;
        private long _sequenceId;
        private IRabbitMqConsumer _consumer;

        public RabbitMqAdapterReceiver(IRabbitMqConnectorFactory rmqConnectorFactory, QueueId queueId, IBatchContainerSerializer serializer, TimeSpan cacheFillingTimeout)
        {
            _rmqConnectorFactory = rmqConnectorFactory;
            _queueId = queueId;
            _serializer = serializer;
            _cacheFillingTimeout = cacheFillingTimeout;
            _sequenceId = 0;
        }

        public Task Initialize(TimeSpan timeout)
        {
            _consumer = _rmqConnectorFactory.CreateConsumer(_queueId);
            return Task.CompletedTask;
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var batch = new List<IBatchContainer>();
            var startTimestamp = DateTime.UtcNow;
            for (int count = 0; count < maxCount || maxCount == QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG; count++)
            {
                // in case of high latency on the network, new messages will be coming in a low rate and until the cache
                // will be filled, we would will be looping here; the timeout is here to break the loop and start the
                // consumption if it takes too long to fill the cache
                if (DateTime.UtcNow - startTimestamp > _cacheFillingTimeout) break;

                // on a very slow network with high latency, the synchronous RMQ Receive will block all worker threads until
                // the RMQ queue is empty or the cache is full; in order to enforce the consumption, the Yield is called,
                // which foces asynchronicity and allows other scheduled methods (the consumers) to continue;
                // the right ways would be to await a ReceiveAsync, but there is currently no such method in RMQ client library;
                // we could only wrap the call in Task.Run, which is also a bad practice
                await Task.Yield();

                var item = _consumer.Receive();
                if (item == null) break;
                try
                {
                    batch.Add(RabbitMqDataAdapter.FromQueueMessage(_serializer, item.Body, _sequenceId++, item.DeliveryTag));
                }
                catch (Exception ex)
                {
                    _rmqConnectorFactory.Logger.Log(0, Runtime.Severity.Error, "GetQueueMessagesAsync: failed to deserialize the message! The message will be thrown away (by calling ACK).", null, ex);
                    _consumer.Ack(item.DeliveryTag);
                }
            }
            return batch;
        }

        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            foreach (var msg in messages)
            {
                var failed = ((RabbitMqBatchContainer) msg).DeliveryFailure;
                var tag = ((RabbitMqBatchContainer) msg).DeliveryTag;
                if (failed)
                {
                    _rmqConnectorFactory.Logger.Log(0, Runtime.Severity.Verbose, $"MessagesDeliveredAsync NACK #{tag} {msg.SequenceToken}", null, null);
                    _consumer.Nack(tag);
                }
                else
                {
                    _rmqConnectorFactory.Logger.Log(0, Runtime.Severity.Verbose, $"MessagesDeliveredAsync ACK #{tag} {msg.SequenceToken}", null, null);
                    _consumer.Ack(tag);
                }
            }
            return Task.CompletedTask;
        }

        public Task Shutdown(TimeSpan timeout)
        {
            _consumer.Dispose();
            return Task.CompletedTask;
        }
    }
}