using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Orleans.Streams.Cache
{
    /// <summary>
    /// A queue cache that keeps items in memory
    /// 
    /// The dictionary is net kept clean, so if there is huge number of random streams the dictionary will keep growing
    /// </summary>
    public class ConcurrentQueueCache : IQueueCache
    {
        // StreamIdentity = <StreamGuid, StreamNamespace> = <Guid, string>
        private readonly ConcurrentDictionary<Tuple<Guid, string>, ConcurrentQueue<RabbitMqBatchContainer>> _cache;
        private readonly ConcurrentQueue<IBatchContainer> _itemsToPurge;
        private readonly int _maxCacheSize;

        private int _numItemsInCache;

        public ConcurrentQueueCache(int cacheSize)
        {
            _maxCacheSize = cacheSize;
            _cache = new ConcurrentDictionary<Tuple<Guid, string>, ConcurrentQueue<RabbitMqBatchContainer>>();
            _itemsToPurge = new ConcurrentQueue<IBatchContainer>();
        }

        /// <summary>
        /// The limit of the maximum number of items that can be added.
        /// 
        /// Returns just an estimate. It doesn't have to be exact. It would require extra locking.
        /// 
        /// Note: there is a condition in pulling agent that if maxAddCount is less than 0 and not equal to -1 (unlimited),
        ///       it will skip the reading cycle including cache purging
        /// </summary>
        public int GetMaxAddCount() => Math.Max(1, _maxCacheSize - _numItemsInCache);

        public bool IsUnderPressure() => _numItemsInCache >= _maxCacheSize;

        public void AddToCache(IList<IBatchContainer> messages)
        {
            foreach (var msg in messages)
            {
                _cache.GetOrAdd(new Tuple<Guid, string>(msg.StreamGuid, msg.StreamNamespace),
                        new ConcurrentQueue<RabbitMqBatchContainer>())
                    .Enqueue((RabbitMqBatchContainer) msg);
            }
            Interlocked.Add(ref _numItemsInCache, messages.Count);
        }

        public bool TryPurgeFromCache(out IList<IBatchContainer> purgedItems)
        {
            purgedItems = null;
            if (!_itemsToPurge.IsEmpty)
            {
                purgedItems = new List<IBatchContainer>();
                while (_itemsToPurge.TryDequeue(out var item))
                {
                    purgedItems.Add(item);
                }
                Interlocked.Add(ref _numItemsInCache, -purgedItems.Count);
            }
            return purgedItems?.Count > 0;
        }

        /// <summary>
        /// Here we ignore the token since this is not rewindable stream anyway
        /// </summary>
        public IQueueCacheCursor GetCacheCursor(IStreamIdentity streamIdentity, StreamSequenceToken token)
        {
            return new ConcurrentQueueCacheCursor(
                moveNext: () =>
                {
                    RabbitMqBatchContainer item = null;
                    _cache.TryGetValue(new Tuple<Guid, string>(streamIdentity.Guid, streamIdentity.Namespace), out var queue);
                    queue?.TryDequeue(out item);
                    return item;
                },
                purgeItem: item =>
                {
                    if (item != null)
                    {
                        _itemsToPurge.Enqueue(item);
                    }
                });
        }
    }
}
