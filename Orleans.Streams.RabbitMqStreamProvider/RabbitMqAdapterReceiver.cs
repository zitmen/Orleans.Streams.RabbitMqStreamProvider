using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Streams.RabbitMq;

namespace Orleans.Streams
{
    internal class RabbitMqAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly IRabbitMqConnectorFactory _rmqConnectorFactory;
        private readonly QueueId _queueId;
        private long _sequenceId;
        private IRabbitMqConsumer _consumer;

        public RabbitMqAdapterReceiver(IRabbitMqConnectorFactory rmqConnectorFactory, QueueId queueId)
        {
            _rmqConnectorFactory = rmqConnectorFactory;
            _queueId = queueId;
            _sequenceId = 0;
        }

        public Task Initialize(TimeSpan timeout)
        {
            _consumer = _rmqConnectorFactory.CreateConsumer(_queueId);
            return TaskDone.Done;
        }

        public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var batch = new List<IBatchContainer>();
            for (int count = 0; count < maxCount || maxCount == QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG; count++)
            {
                var item = _consumer.Receive();
                if (item == null) break;
                batch.Add(RabbitMqDataAdapter.FromQueueMessage(item.Body, _sequenceId++, item.DeliveryTag));
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
            return TaskDone.Done;
        }

        public Task Shutdown(TimeSpan timeout)
        {
            _consumer.Dispose();
            return TaskDone.Done;
        }
    }
}