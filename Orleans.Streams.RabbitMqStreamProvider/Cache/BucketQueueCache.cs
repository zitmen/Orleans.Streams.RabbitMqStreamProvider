using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Runtime;

namespace Orleans.Streams.Cache
{
    internal class CacheBucket
    {
        // For backpressure detection we maintain a histogram of 10 buckets.
        // Every bucket records how many items are in the cache in that bucket
        // and how many cursors are pointing to an item in that bucket.
        // We update the NumCurrentItems when we add and remove cache item (potentially opening or removing a bucket)
        // We update NumCurrentCursors every time we move a cursor
        // If the first (most outdated bucket) has at least one cursor pointing to it, we say we are under back pressure (in a full cache).
        internal int NumCurrentItems { get; private set; }
        internal int NumCurrentCursors { get; private set; }
        internal bool ConsumptionStarted { get; private set; } // we don't want to drain a bucket which doesn't have any active cursor in a case
                                                               // when the consumption didn't start yet - this should solve concurrency issues

        internal void UpdateNumItems(int val)
        {
            NumCurrentItems = NumCurrentItems + val;
        }

        private readonly object _syncRoot = new object();
        
        internal void UpdateNumCursors(int val)
        {
            lock (_syncRoot)
            {
                ConsumptionStarted = ConsumptionStarted || val > 0;
                NumCurrentCursors = NumCurrentCursors + val;
            }
        }
    }

    internal class QueueCacheItem
    {
        internal IBatchContainer Batch;
        internal bool DeliveryFailure;
        internal StreamSequenceToken SequenceToken;
        internal CacheBucket CacheBucket;
    }

    /// <summary>
    /// A queue cache that keeps items in memory
    /// </summary>
    public class BucketQueueCache : IQueueCache
    {
        private readonly LinkedList<QueueCacheItem> _cachedMessages;
        private readonly int _maxCacheSize;
        private readonly Logger _logger;
        private readonly List<CacheBucket> _cacheCursorHistogram; // for backpressure detection
        private readonly int _cacheHistogramMaxBucketSize;

        /// <summary>
        /// Number of items in the cache
        /// </summary>
        public int Size => _cachedMessages.Count;

        /// <summary>
        /// The limit of the maximum number of items that can be added
        /// 
        /// Note: there is a condition in pulling agent that if maxAddCount is less than 0 and not equal to -1 (unlimited),
        ///       it will skip the reading cycle including cache purging
        /// </summary>
        public int GetMaxAddCount()
            => Math.Max(Math.Min(_cacheHistogramMaxBucketSize, _maxCacheSize - Size), 1);
        
        /// <summary>
        /// Returns true if this cache is under pressure.
        /// </summary>
        public virtual bool IsUnderPressure()
            => Size >= _maxCacheSize;

        /// <summary>
        /// MySimpleQueueCache Constructor
        /// </summary>
        public BucketQueueCache(int cacheSize, int cacheBuckets, Logger logger)
        {
            _cachedMessages = new LinkedList<QueueCacheItem>();
            _cacheCursorHistogram = new List<CacheBucket>();
            _maxCacheSize = cacheSize;
            _logger = logger;

            var numCacheHistogramBuckets = Math.Min(_maxCacheSize, cacheBuckets);
            _cacheHistogramMaxBucketSize = Math.Max(cacheSize / numCacheHistogramBuckets, 1);
        }
        
        /// <summary>
        /// Ask the cache if it has items that can be purged from the cache 
        /// (so that they can be subsequently released them the underlying queue).
        /// </summary>
        /// <param name="purgedItems"></param>
        public virtual bool TryPurgeFromCache(out IList<IBatchContainer> purgedItems)
        {
            purgedItems = null;
            if (_cachedMessages.Count == 0) return false; // empty cache
            if (_cacheCursorHistogram.Count == 0) return false;  // no cursors yet - zero consumers basically yet.
            if (!_cacheCursorHistogram.Any(bucket => bucket.ConsumptionStarted && bucket.NumCurrentCursors == 0)) return false; // consumers are still active in all buckets

            purgedItems = new List<IBatchContainer>();
            var node = _cachedMessages.Last;
            while (node != null)
            {
                var item = node.Value;
                var prev = node.Previous;
                if (item.CacheBucket.ConsumptionStarted && item.CacheBucket.NumCurrentCursors == 0)
                {
                    ((RabbitMqBatchContainer)item.Batch).DeliveryFailure = item.DeliveryFailure;
                    purgedItems.Add(item.Batch);
                    _cachedMessages.Remove(node);
                    item.CacheBucket.UpdateNumItems(-1);
                }
                node = prev;
            }
            _cacheCursorHistogram.RemoveAll(bucket => bucket.ConsumptionStarted && bucket.NumCurrentCursors == 0 && bucket.NumCurrentItems == 0);

            Log(_logger, "TryPurgeFromCache: purged {0} items from cache.", purgedItems.Count);
            return true;
        }

        /// <summary>
        /// Add a list of message to the cache
        /// </summary>
        /// <param name="msgs"></param>
        public virtual void AddToCache(IList<IBatchContainer> msgs)
        {
            if (msgs == null) throw new ArgumentNullException(nameof(msgs));

            Log(_logger, "AddToCache: added {0} items to cache.", msgs.Count);
            foreach (var message in msgs)
            {
                Add(message, message.SequenceToken);
            }
        }

        /// <summary>
        /// Acquire a stream message cursor.  This can be used to retreave messages from the
        ///   cache starting at the location indicated by the provided token.
        /// </summary>
        /// <param name="streamIdentity"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public virtual IQueueCacheCursor GetCacheCursor(IStreamIdentity streamIdentity, StreamSequenceToken token)
        {
            var cursor = new BucketQueueCacheCursor(this, streamIdentity, _logger);
            InitializeCursor(cursor, token);
            return cursor;
        }

        internal void InitializeCursor(BucketQueueCacheCursor cursor, StreamSequenceToken sequenceToken)
        {
            Log(_logger, "InitializeCursor: {0} to sequenceToken {1}", cursor, sequenceToken);

            // Nothing in cache, unset token, and wait for more data.
            if (_cachedMessages.Count == 0)
            {
                UnsetCursor(cursor, sequenceToken);
                return;
            }

            // if no token is provided, set cursor to idle at end of cache
            if (sequenceToken == null)
            {
                UnsetCursor(cursor, _cachedMessages.First?.Value?.SequenceToken);
                return;
            }

            // If sequenceToken is too new to be in cache, unset token, and wait for more data.
            if (sequenceToken.Newer(_cachedMessages.First.Value.SequenceToken))
            {
                UnsetCursor(cursor, sequenceToken);
                return;
            }

            LinkedListNode<QueueCacheItem> lastMessage = _cachedMessages.Last;
            // Check to see if offset is too old to be in cache
            if (sequenceToken.Older(lastMessage.Value.SequenceToken))
            {
                throw new QueueCacheMissException(sequenceToken, _cachedMessages.Last.Value.SequenceToken, _cachedMessages.First.Value.SequenceToken);
            }

            // Now the requested sequenceToken is set and is also within the limits of the cache.

            // Find first message at or below offset
            // Events are ordered from newest to oldest, so iterate from start of list until we hit a node at a previous offset, or the end.
            LinkedListNode<QueueCacheItem> node = _cachedMessages.First;
            while (node != null && node.Value.SequenceToken.Newer(sequenceToken))
            {
                // did we get to the end?
                if (node.Next == null) // node is the last message
                    break;

                // if sequenceId is between the two, take the higher
                if (node.Next.Value.SequenceToken.Older(sequenceToken))
                    break;

                node = node.Next;
            }

            // return cursor from start.
            SetCursor(cursor, node);
        }

        internal void RefreshCursor(BucketQueueCacheCursor cursor, StreamSequenceToken sequenceToken)
        {
            Log(_logger, "RefreshCursor: {0} to sequenceToken {1}", cursor, sequenceToken);

            // set if unset
            if (!cursor.IsSet)
            {
                InitializeCursor(cursor, cursor.SequenceToken ?? sequenceToken);
            }
        }

        /// <summary>
        /// Aquires the next message in the cache at the provided cursor
        /// </summary>
        /// <param name="cursor"></param>
        /// <param name="batch"></param>
        /// <returns></returns>
        internal bool TryGetNextMessage(BucketQueueCacheCursor cursor, out QueueCacheItem batch)
        {
            Log(_logger, "TryGetNextMessage: {0}", cursor);

            batch = null;
            if (!cursor.IsSet) return false;

            // Capture the current element and advance to the next one.
            batch = cursor.Element.Value;

            // If we are at the end of the cache unset cursor and move offset one forward
            if (cursor.Element == _cachedMessages.First)
            {
                UnsetCursor(cursor, null);
            }
            else // Advance to next:
            {
                AdvanceCursor(cursor, cursor.Element.Previous);
            }
            return true;
        }

        private void AdvanceCursor(BucketQueueCacheCursor cursor, LinkedListNode<QueueCacheItem> item)
        {
            Log(_logger, "UpdateCursor: {0} to item {1}", cursor, item?.Value.Batch);

            cursor.Set(item);
        }

        internal void SetCursor(BucketQueueCacheCursor cursor, LinkedListNode<QueueCacheItem> item)
        {
            Log(_logger, "SetCursor: {0} to item {1}", cursor, item?.Value.Batch);

            cursor.Set(item);
        }

        internal void UnsetCursor(BucketQueueCacheCursor cursor, StreamSequenceToken token)
        {
            Log(_logger, "UnsetCursor: {0}", cursor);

            cursor.UnSet(token);
        }

        private void Add(IBatchContainer batch, StreamSequenceToken sequenceToken)
        {
            if (batch == null) throw new ArgumentNullException(nameof(batch));
            // this should never happen, but just in case
            if (Size >= _maxCacheSize) throw new CacheFullException();

            CacheBucket cacheBucket;
            if (_cacheCursorHistogram.Count == 0)
            {
                cacheBucket = new CacheBucket();
                _cacheCursorHistogram.Add(cacheBucket);
            }
            else
            {
                cacheBucket = _cacheCursorHistogram[_cacheCursorHistogram.Count - 1]; // last one
            }

            if (cacheBucket.NumCurrentItems == _cacheHistogramMaxBucketSize) // last bucket is full, open a new one
            {
                cacheBucket = new CacheBucket();
                _cacheCursorHistogram.Add(cacheBucket);
            }

            // Add message to linked list
            var item = new QueueCacheItem
            {
                Batch = batch,
                SequenceToken = sequenceToken,
                CacheBucket = cacheBucket
            };

            _cachedMessages.AddFirst(new LinkedListNode<QueueCacheItem>(item));
            cacheBucket.UpdateNumItems(1);
        }

        internal static void Log(Logger logger, string format, params object[] args)
        {
            if (logger.IsVerbose) logger.Verbose(format, args);
        }
    }
}
