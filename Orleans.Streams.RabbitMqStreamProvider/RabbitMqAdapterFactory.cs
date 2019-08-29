using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams.BatchContainer;
using Orleans.Streams.Cache;

namespace Orleans.Streams
{
    public class RabbitMqAdapterFactory<TSerializer> : IQueueAdapterFactory where TSerializer : IBatchContainerSerializer, new()
    {
        private readonly IQueueAdapterCache _cache;
        private readonly IStreamQueueMapper _mapper;
        private readonly Task<IStreamFailureHandler> _failureHandler;
        private readonly IQueueAdapter _adapter;
        
        public RabbitMqAdapterFactory(
            string providerName,
            RabbitMqOptions rmqOptions,
            CachingOptions cachingOptions,
            IServiceProvider serviceProvider,
            ILoggerFactory loggerFactory)
        {
            if (string.IsNullOrEmpty(providerName)) throw new ArgumentNullException(nameof(providerName));
            if (rmqOptions == null) throw new ArgumentNullException(nameof(rmqOptions));
            if (cachingOptions == null) throw new ArgumentNullException(nameof(cachingOptions));
            if (serviceProvider == null) throw new ArgumentNullException(nameof(serviceProvider));
            if (loggerFactory == null) throw new ArgumentNullException(nameof(loggerFactory));

            _cache = new SimpleQueueAdapterCache(new SimpleQueueCacheOptions { CacheSize = cachingOptions.CacheSize }, providerName, loggerFactory);
            _mapper = new HashRingBasedStreamQueueMapper(new HashRingStreamQueueMapperOptions { TotalQueueCount = rmqOptions.NumberOfQueues }, rmqOptions.QueueNamePrefix);
            _failureHandler = Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler(false));

            var serializer = typeof(TSerializer) == typeof(DefaultBatchContainerSerializer)
                ? new DefaultBatchContainerSerializer(serviceProvider.GetRequiredService<SerializationManager>())
                : (IBatchContainerSerializer)new TSerializer();

            _adapter = new RabbitMqAdapter(rmqOptions, cachingOptions, serializer, _mapper, providerName, loggerFactory);
        }

        public Task<IQueueAdapter> CreateAdapter() => Task.FromResult(_adapter);
        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId) => _failureHandler;
        public IQueueAdapterCache GetQueueAdapterCache() => _cache;
        public IStreamQueueMapper GetStreamQueueMapper() => _mapper;

        public static RabbitMqAdapterFactory<TSerializer> Create(IServiceProvider services, string name)
            => ActivatorUtilities.CreateInstance<RabbitMqAdapterFactory<TSerializer>>(
                services,
                name,
                services.GetOptionsByName<RabbitMqOptions>(name),
                services.GetOptionsByName<CachingOptions>(name));
    }
}