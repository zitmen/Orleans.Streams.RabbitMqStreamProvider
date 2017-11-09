using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Concurrency;
using Orleans.Storage;
using Orleans.Streams;
using Orleans.TestingHost;

namespace RabbitMqStreamTests
{
    [TestClass]
    public class RmqIntegrationTests
    {
        [Ignore]
        [TestMethod]
        public async Task TestConcurrentProcessingWithPrefilledQueue()
        {
            await _cluster.StopPullingAgents();

            var rand = new Random();
            var messages = Enumerable.Range(1, 1000).Select(id => new Message(id, rand.Next(1, 5) * 1000)).ToArray();

            var aggregator = _cluster.GrainFactory.GetGrain<IAggregatorGrain>(Guid.Empty);
            await aggregator.CleanUp(); // has to be done here, because the sender is also accessing the aggregator

            var sender = _cluster.GrainFactory.GetGrain<ISenderGrain>(Guid.Empty);
            await Task.WhenAll(messages.Select(msg => sender.SendMessage(msg.AsImmutable())));

            await _cluster.StartPullingAgents();

            int iters = 0;
            while (!await AllMessagesSentAndDelivered(aggregator, messages) && iters < 10)
            {
                //iters++;
                await Task.Delay(TimeSpan.FromSeconds(10));
            }

            Assert.IsTrue(await AllMessagesSentAndDelivered(aggregator, messages));
            Assert.AreEqual(2, await aggregator.GetProcessingSilosCount());
        }

        [Ignore]
        [TestMethod]
        public async Task TestConcurrentProcessingOnFly()
        {
            var rand = new Random();
            var messages = Enumerable.Range(1, 1000).Select(id => new Message(id, rand.Next(1, 5) * 1000)).ToArray();

            var aggregator = _cluster.GrainFactory.GetGrain<IAggregatorGrain>(Guid.Empty);
            await aggregator.CleanUp(); // has to be done here, because the sender is also accessing the aggregator

            var sender = _cluster.GrainFactory.GetGrain<ISenderGrain>(Guid.Empty);
            await Task.WhenAll(messages.Select(msg => sender.SendMessage(msg.AsImmutable())));

            int iters = 0;
            while (!await AllMessagesSentAndDelivered(aggregator, messages) && iters < 10)
            {
                //iters++;
                await Task.Delay(TimeSpan.FromSeconds(10));
            }

            Assert.IsTrue(await AllMessagesSentAndDelivered(aggregator, messages));
            Assert.AreEqual(2, await aggregator.GetProcessingSilosCount());
        }

        private static async Task<bool> AllMessagesSentAndDelivered(IAggregatorGrain aggregator, Message[] messages)
            => await aggregator.WereAllMessagesSent(messages.AsImmutable()) &&
               await aggregator.WereAllSentAlsoDelivered();


        #region Test cluster control

        private static TestClusterOptions CreateClusterOptions()
        {
            var options = new TestClusterOptions(2);

            options.ClusterConfiguration.Globals.UseLivenessGossip = false;
            options.ClusterConfiguration.Globals.RegisterStorageProvider<MemoryStorage>("PubSubStore");
            options.ClusterConfiguration.Globals.RegisterStreamProvider<RabbitMqStreamProvider>(Globals.StreamProviderName,
                new Dictionary<string, string>
                {
                    { "HostName", "localhost" },
                    { "Port", "5672" },
                    { "VirtualHost", "/" },
                    { "UserName", "guest" },
                    { "Password", "guest" },
                    { "GetQueueMessagesTimerPeriod", "100ms" },
                    { "QueueNamePrefix", "test" },
                    { "NumberOfQueues", "2" },
                    { "CacheSize", "100" },
                    { PersistentStreamProviderConfig.STREAM_PUBSUB_TYPE, StreamPubSubType.ImplicitOnly.ToString() }
                });

            return options;
        }

        private static TestCluster _cluster;

        [ClassInitialize]
        public static void ClassInitialize(TestContext context)
        {
            _cluster = CreateClusterOptions().CreateTestCluster();
            // TODO: ensure empty RMQ
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            _cluster.Shutdown();
        }

        #endregion
    }
}
