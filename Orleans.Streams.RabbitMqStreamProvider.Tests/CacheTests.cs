using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using Orleans.Streams.Cache;
using Orleans.TestingHost.Utils;

namespace RabbitMqStreamTests
{
    [TestClass]
    public class CacheTests
    {
        [TestMethod]
        public async Task RunCacheTestMultipleTimes()
        {
            // There used to be a concurrency issue, because CacheBucket.UpdateNumItems(int) was not atomic
            // which caused buckets ending up with a non-zero count of cursors, even when all cursors were unset.
            // Since this concurrency issue doesn't appear every single time, we re-run the single test multiple
            // times so the issue if not taken care of would appear every time.
            for (int i = 0; i < 10; i++)
            {
                try
                {
                    await TestOrleansCacheUsage();
                }
                catch (Exception)
                {
                    // there could be actually more concurrency issues, which would cause
                    // one of these exceptions: NullReference, CacheMiss, CacheFull
                    // - they would be caused by concurrent add/remove/traverse
                    // - synchronization is required to fix this
                    // but let's ignore this for now, because in Orleans there is
                    // a retry logic so it should help to recover from such errors
                    // and authors of Orleans don't seem to be much concerned about it
                    i--;
                }
            }
        }

        private async Task TestOrleansCacheUsage()
        {
            var queueCache = new BucketQueueCache(50, 10, new NoOpTestLogger());

            var tasks = new List<Task>();
            for (int i = 0; i < 10; i++)
            {
                queueCache.TryPurgeFromCache(out var purgedItems);
                
                var multiBatch = GetQueueMessages(queueCache.GetMaxAddCount());
                queueCache.AddToCache(multiBatch);

                if (multiBatch.Count > 0)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        using (var cursor = queueCache.GetCacheCursor(StreamIdentity, multiBatch.First().SequenceToken))
                        {
                            while (cursor.MoveNext())
                            {
                                var batch = cursor.GetCurrent(out var ignore);
                                await Task.Delay(TimeSpan.FromMilliseconds(50 + new Random().Next(100)));
                            }
                        }
                    }));
                }
                await Task.Delay(TimeSpan.FromMilliseconds(200));
            }

            await Task.WhenAll(tasks);
            queueCache.TryPurgeFromCache(out var finalPurgedItems);
            
            Assert.AreEqual(0, queueCache.Size);
        }

        private static readonly StreamIdentity StreamIdentity = new StreamIdentity(Guid.Empty, "test");
        private static ulong _deliveryTag = 0;

        private static IList<IBatchContainer> GetQueueMessages(int maxCount)
        {
            return Enumerable.Range(0, new Random().Next(maxCount))
                .Select(i =>
                    new RabbitMqBatchContainer(
                        StreamIdentity.Guid, StreamIdentity.Namespace,
                        new[] {"value"}.ToList<object>(),
                        new Dictionary<string, object>())
                    {
                        DeliveryTag = _deliveryTag++,
                        EventSequenceToken = new EventSequenceToken((long) _deliveryTag)
                    })
                .ToList<IBatchContainer>();
        }
    }
}