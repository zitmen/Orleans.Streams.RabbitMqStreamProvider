using System;
using System.Collections.Concurrent;

namespace Orleans.Streams.Cache
{
    public class ConcurrentQueueAdapterCache : IQueueAdapterCache
    {
        private readonly int _cacheSize;
        private readonly ConcurrentDictionary<QueueId, IQueueCache> _caches;
        
        public ConcurrentQueueAdapterCache(int cacheSize)
        {
            if (cacheSize <= 0) throw new ArgumentOutOfRangeException(nameof(cacheSize), "CacheSize must be a positive number.");
            _cacheSize = cacheSize;
            _caches = new ConcurrentDictionary<QueueId, IQueueCache>();
        }

        /// <summary>
        /// Create a cache for a given queue id
        /// </summary>
        /// <param name="queueId"></param>
        public IQueueCache CreateQueueCache(QueueId queueId)
        {
            return _caches.AddOrUpdate(queueId,
                id => new ConcurrentQueueCache(_cacheSize),
                (id, queueCache) => queueCache);
        }
    }
}
