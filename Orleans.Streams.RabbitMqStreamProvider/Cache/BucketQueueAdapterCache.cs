using System;
using System.Collections.Concurrent;
using Orleans.Runtime;

namespace Orleans.Streams.Cache
{
    public class BucketQueueAdapterCache : IQueueAdapterCache
    {
        private readonly int _cacheSize;
        private readonly int _cacheBuckets;
        private readonly Logger _logger;
        private readonly ConcurrentDictionary<QueueId, IQueueCache> _caches;

        /// <summary>
        /// Adapter for simple queue caches
        /// </summary>
        /// <param name="cacheSize"></param>
        /// <param name="cacheBuckets"></param>
        /// <param name="logger"></param>
        public BucketQueueAdapterCache(int cacheSize, int cacheBuckets, Logger logger)
        {
            if (cacheSize <= 0) throw new ArgumentOutOfRangeException(nameof(cacheSize), "CacheSize must be a positive number.");
            _cacheSize = cacheSize;
            _cacheBuckets = cacheBuckets;
            _logger = logger;
            _caches = new ConcurrentDictionary<QueueId, IQueueCache>();
        }

        /// <summary>
        /// Create a cache for a given queue id
        /// </summary>
        /// <param name="queueId"></param>
        public IQueueCache CreateQueueCache(QueueId queueId)
        {
            return _caches.AddOrUpdate(queueId, (id) => new BucketQueueCache(_cacheSize, _cacheBuckets, _logger), (id, queueCache) => queueCache);
        }
    }
}
