using System;
using System.Collections.Generic;
using Orleans.Runtime;

namespace Orleans.Streams.Cache
{
    /// <summary>
    /// Cursor into a simple queue cache
    /// 
    /// Note: this is an exact copy of SimpleQueueCacheCursor from Orleans 1.4.1
    /// </summary>
    public class BucketQueueCacheCursor : IQueueCacheCursor
    {
        private readonly IStreamIdentity _streamIdentity;
        private readonly BucketQueueCache _cache;
        private readonly Logger _logger;
        private QueueCacheItem _current; // this is a pointer to the current element in the cache. It is what will be returned by GetCurrent().

        // This is a pointer to the NEXT element in the cache.
        // After the cursor is first created it should be called MoveNext before the call to GetCurrent().
        // After MoveNext returns, the current points to the current element that will be returned by GetCurrent()
        // and Element will point to the next element (since MoveNext actualy advanced it to the next).
        internal LinkedListNode<QueueCacheItem> Element { get; private set; }
        internal StreamSequenceToken SequenceToken { get; private set; }

        internal bool IsSet => Element != null;

        internal void Set(LinkedListNode<QueueCacheItem> item)
        {
            if (item == null) throw new NullReferenceException(nameof(item));
            Element = item;
            SequenceToken = item.Value.SequenceToken;
        }

        internal void UnSet(StreamSequenceToken token)
        {
            Element = null;
            SequenceToken = token;
        }

        /// <summary>
        /// Cursor into a simple queue cache
        /// </summary>
        /// <param name="cache"></param>
        /// <param name="streamIdentity"></param>
        /// <param name="logger"></param>
        public BucketQueueCacheCursor(BucketQueueCache cache, IStreamIdentity streamIdentity, Logger logger)
        {
            _cache = cache;
            _streamIdentity = streamIdentity;
            _logger = logger;
            _current = null;
            BucketQueueCache.Log(logger, "MySimpleQueueCacheCursor New Cursor for {0}, {1}", streamIdentity.Guid, streamIdentity.Namespace);
        }

        /// <summary>
        /// Get the current value.
        /// </summary>
        /// <param name="exception"></param>
        /// <returns>
        /// Returns the current batch container.
        /// If null then the stream has completed or there was a stream error.  
        /// If there was a stream error, an error exception will be provided in the output.
        /// </returns>
        public virtual IBatchContainer GetCurrent(out Exception exception)
        {
            BucketQueueCache.Log(_logger, "MySimpleQueueCacheCursor.GetCurrent: {0}", _current);

            exception = null;
            return _current.Batch;
        }

        /// <summary>
        /// Move to next message in the stream.
        /// If it returns false, there are no more messages.  The enumerator is still
        ///  valid howerver and can be called again when more data has come in on this
        ///  stream.
        /// </summary>
        /// <returns></returns>
        public virtual bool MoveNext()
        {
            _current?.CacheBucket.UpdateNumCursors(-1); // remove from prev bucket

            QueueCacheItem next;
            while (_cache.TryGetNextMessage(this, out next))
            {
                if (IsInStream(next.Batch))
                    break;
            }
            if (next == null || !IsInStream(next.Batch))
                return false;

            _current = next;
            _current.CacheBucket.UpdateNumCursors(1);  // add to next bucket
            return true;
        }

        /// <summary>
        /// Refresh that cache cursor. Called when new data is added into a cache.
        /// </summary>
        /// <returns></returns>
        public virtual void Refresh(StreamSequenceToken sequenceToken)
        {
            if (!IsSet)
            {
                _cache.RefreshCursor(this, sequenceToken);
            }
        }

        /// <summary>
        /// Record that delivery of the current event has failed
        /// </summary>
        public void RecordDeliveryFailure()
        {
            if (IsSet && _current != null)
            {
                Element.Value.DeliveryFailure = true;
            }
        }

        private bool IsInStream(IBatchContainer batchContainer)
        {
            return batchContainer != null &&
                    batchContainer.StreamGuid.Equals(_streamIdentity.Guid) &&
                    string.Equals(batchContainer.StreamNamespace, _streamIdentity.Namespace);
        }

        #region IDisposable Members

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Clean up cache data when done
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _cache.UnsetCursor(this, null);
            }
        }

        #endregion

        /// <summary>
        /// Convert object to string
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return $"<MySimpleQueueCacheCursor: Element={Element?.Value.Batch.ToString() ?? "null"}, SequenceToken={SequenceToken?.ToString() ?? "null"}>";
        }
    }
}