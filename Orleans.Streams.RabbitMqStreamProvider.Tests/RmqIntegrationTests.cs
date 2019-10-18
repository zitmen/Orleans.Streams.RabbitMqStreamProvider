using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using static RabbitMqStreamTests.ToxiProxyHelpers;

namespace RabbitMqStreamTests
{
    [TestClass]
    public class RmqIntegrationTests
    {
        [TestMethod]
        public async Task TestConcurrentProcessingWithPrefilledQueue()
        {
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                setupProxyForReceiver: conn => { },
                setupProxyForSender: conn => { },
                nMessages: 1000,
                itersToWait: 20);
        }

        [TestMethod]
        public async Task TestConcurrentProcessingOnFly()
        {
            await _cluster.TestRmqStreamProviderOnFly(
                setupProxy: conn => { },
                nMessages: 1000,
                itersToWait: 20);
        }

        [TestMethod]
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

        [TestInitialize]
        public void TestInitialize()
        {
            RmqHelpers.EnsureEmptyQueue();
        }

        [ClassInitialize]
        public static void ClassInitialize(TestContext context)
        {
            // ToxiProxy
            _proxyProcess = StartProxy();

            // Orleans cluster
            _cluster = TestCluster.Create().GetAwaiter().GetResult();

            // try to wait little longer in case everything is slow
            Task.Delay(TimeSpan.FromMinutes(1)).GetAwaiter().GetResult();
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            // close first to avoid a case where Silo hangs, I stop the test and the proxy process keeps running
            _proxyProcess.Terminate();

            _cluster.Shutdown().GetAwaiter().GetResult();
        }

        #endregion
    }
}
