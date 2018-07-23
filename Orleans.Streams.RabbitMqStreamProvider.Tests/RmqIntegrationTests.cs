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
                conn => { },
                conn => { },
                1000, 10);
        }

        [TestMethod]
        public async Task TestConcurrentProcessingOnFly()
        {
            await _cluster.TestRmqStreamProviderOnFly(
                conn => { },
                1000, 10);
        }

        [TestMethod]
        public async Task TestConcurrentProcessingOnFlyWithCustomSerializer()
        {
            await _cluster.TestRmqStreamProviderOnFly(
                conn => { },
                1000, 10, RmqSerializer.ProtoBuf);
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
            _cluster = Task.Run(() => TestCluster.Create()).Result;
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            // close first to avoid a case where Silo hangs, I stop the test and the proxy process keeps running
            _proxyProcess.CloseMainWindow();
            _proxyProcess.WaitForExit();

            _cluster.Shutdown();
        }

        #endregion
    }
}
