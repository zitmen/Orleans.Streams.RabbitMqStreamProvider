using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.TestingHost;
using static RabbitMqStreamTests.ToxiProxyHelpers;
using static RabbitMqStreamTests.TestClusterUtils;

namespace RabbitMqStreamTests
{
    [TestClass]
    public class RmqIntegrationSerializerTests
    {
        [TestMethod]
        public async Task TestConcurrentProcessingOnFly()
        {
            await _cluster.TestRmqStreamProviderOnFly(
                conn => { },
                1000, 10);
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
            _cluster = CreateTestCluster(RmqSerializer.ProtoBuf);
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
