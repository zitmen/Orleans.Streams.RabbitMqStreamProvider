using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Serialization;
using Orleans.Streams.RabbitMq;

namespace Orleans.Streams
{
    internal class RabbitMqAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly IRabbitMqConnectorFactory _rmqConnectorFactory;
        private readonly QueueId _queueId;
        private readonly SerializationManager _serializationManager;
        private long _sequenceId;
        private IRabbitMqConsumer _consumer;

        public RabbitMqAdapterReceiver(IRabbitMqConnectorFactory rmqConnectorFactory, QueueId queueId, SerializationManager serializationManager)
        {
            _rmqConnectorFactory = rmqConnectorFactory;
            _queueId = queueId;
            _serializationManager = serializationManager;
            _sequenceId = 0;
        }

        public Task Initialize(TimeSpan timeout)
        {
            _consumer = _rmqConnectorFactory.CreateConsumer(_queueId);
            return Task.CompletedTask;
        }

        public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var batch = new List<IBatchContainer>();
            for (int count = 0; count < maxCount || maxCount == QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG; count++)
            {
                var item = _consumer.Receive();
                if (item == null) break;
                try
                {
                    batch.Add(RabbitMqDataAdapter.FromQueueMessage(_serializationManager, item.Body, _sequenceId++, item.DeliveryTag));
                }
                catch (Exception ex)
                {
                    _rmqConnectorFactory.Logger.Log(0, Runtime.Severity.Error, "GetQueueMessagesAsync: failed to deserialize the message! The message will be thrown away (by calling ACK).", null, ex);
                    _consumer.Ack(item.DeliveryTag);
                }
            }
            return Task.FromResult<IList<IBatchContainer>>(batch);
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