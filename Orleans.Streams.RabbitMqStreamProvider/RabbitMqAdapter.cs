using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams.RabbitMq;

namespace Orleans.Streams
{
    /// <summary>
    /// For RMQ client, it is necessary the Model (channel) is not accessed by multiple threads at once, because with each such access,
    /// the channel gets closed - this is a limitation of RMQ client, which unfortunatelly causes message loss.
    /// Here we handle it by creating new connection for each receiver which guarantess no overlapping calls from different threads.
    /// The real issue comes in publishing - here we need to identify the connections not only by QueueId, but also by thread!
    /// Otherwise it would cause a lot of trouble when publishin messagess from StatelessWorkers which can run in parallel, thus
    /// overlapping calls from different threads would occur frequently.
    /// </summary>
    internal class RabbitMqAdapter : IQueueAdapter
    {
        private readonly SerializationManager _serializationManager;
        private readonly IStreamQueueMapper _mapper;
        private readonly ConcurrentDictionary<Tuple<int, QueueId>, IRabbitMqProducer> _queues = new ConcurrentDictionary<Tuple<int, QueueId>, IRabbitMqProducer>();
        private readonly IRabbitMqConnectorFactory _rmqConnectorFactory;

        public RabbitMqAdapter(RabbitMqStreamProviderOptions options, SerializationManager serializationManager, IStreamQueueMapper mapper, string providerName, Logger logger)
        {
            _serializationManager = serializationManager;
            _mapper = mapper;
            Name = providerName;
            _rmqConnectorFactory = new RabbitMqOnlineConnectorFactory(options, logger);
        }

        public string Name { get; }
        public bool IsRewindable => false;
        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;
        public IQueueAdapterReceiver CreateReceiver(QueueId queueId) => new RabbitMqAdapterReceiver(_rmqConnectorFactory, queueId, _serializationManager);

        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            if (token != null) throw new ArgumentException("RabbitMq stream provider does not support non-null StreamSequenceToken.", nameof(token));
            
            var queueId = _mapper.GetQueueForStream(streamGuid, streamNamespace);
            var key = new Tuple<int, QueueId>(Thread.CurrentThread.ManagedThreadId, queueId);
            if (!_queues.TryGetValue(key, out var rmq))
            {
                rmq = _queues.GetOrAdd(key, _rmqConnectorFactory.CreateProducer(queueId));
            }
            rmq.Send(RabbitMqDataAdapter.ToQueueMessage(_serializationManager, streamGuid, streamNamespace, events, requestContext));
            return Task.CompletedTask;
        }
    }
}