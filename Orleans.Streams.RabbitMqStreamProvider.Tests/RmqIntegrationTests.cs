using System;
using System.Diagnostics;
using System.Threading.Tasks;
using NUnit.Framework;
using static RabbitMqStreamTests.ToxiProxyHelpers;

namespace RabbitMqStreamTests
{
    [TestFixture]
    public class RmqIntegrationTests
    {
        [Test]
        public async Task TestConcurrentProcessingWithPrefilledQueue()
        {
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                setupProxyForReceiver: conn => { },
                setupProxyForSender: conn => { },
                nMessages: 1000,
                itersToWait: 20);
        }

        [Test]
        public async Task TestConcurrentProcessingOnFly()
        {
            await _cluster.TestRmqStreamProviderOnFly(
                setupProxy: conn => { },
                nMessages: 1000,
                itersToWait: 20);
        }

        [Test]
        public async Task TestConcurrentProcessingOnFlyWithCustomSerializer()
        {
            await _cluster.TestRmqStreamProviderOnFly(
                setupProxy: conn => { },
                nMessages: 1000,
                itersToWait: 20,
                serializer: RmqSerializer.ProtoBuf);
        }

        #region Test class setup

        private static TestCluster _cluster;
        private static Process _proxyProcess;

        [SetUp]
        public void TestInitialize()
        {
            RmqHelpers.EnsureEmptyQueue();
        }

        [OneTimeSetUp]
        public static async Task ClassInitialize()
        {
            // ToxiProxy
            _proxyProcess = StartProxy();

            // Orleans cluster
            _cluster = await TestCluster.Create();

            // try to wait little longer in case everything is slow
            await Task.Delay(TimeSpan.FromMinutes(1));
        }

        [OneTimeTearDown]
        public static async Task ClassCleanup()
        {
            // close first to avoid a case where Silo hangs, I stop the test and the proxy process keeps running
            _proxyProcess.Terminate();

            await _cluster.Shutdown();
        }

        #endregion
    }
}
