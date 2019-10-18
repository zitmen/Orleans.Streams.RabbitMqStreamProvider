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
                itersToWait: 10);
        }

        [TestMethod]
        public async Task TestConcurrentProcessingOnFly()
        {
            await _cluster.TestRmqStreamProviderOnFly(
                setupProxy: conn => { },
                nMessages: 1000,
                itersToWait: 10);
        }

        [TestMethod]
        public async Task TestConcurrentProcessingOnFlyWithCustomSerializer()
        {
            await _cluster.TestRmqStreamProviderOnFly(
                setupProxy: conn => { },
                nMessages: 1000,
                itersToWait: 10,
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
            _cluster = Task.Run(TestCluster.Create).GetAwaiter().GetResult();
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
