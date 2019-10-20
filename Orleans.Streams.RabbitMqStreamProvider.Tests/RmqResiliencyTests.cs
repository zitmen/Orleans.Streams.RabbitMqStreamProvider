using System;
using System.Diagnostics;
using System.Threading.Tasks;
using NUnit.Framework;
using Toxiproxy.Net.Toxics;
using static RabbitMqStreamTests.ToxiProxyHelpers;

// Note: receiveng seems to be more sensitive to network errors than sending, thus reducing latency in some of the test cases
// Note: when running tests individually they pass; when running in batch, it fails with timeout + there is a problem with shutting down silo -> ignore the test class

namespace RabbitMqStreamTests
{
    [Ignore("not stable")]
    [TestFixture]
    public class RmqResiliencyTests
    {
        #region Timeout

        [Test]
        public async Task TestRmqTimeoutUpstreamWhileSending()
        {
            // tests send call
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => AddTimeoutToRmqProxy(conn, ToxicDirection.UpStream, 0.9, 100),
                conn => { },
                1000, 10);
        }

        [Test]
        public async Task TestRmqTimeoutDownstreamWhileSending()
        {
            // tests (n)ack from the rmq to the client
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => AddTimeoutToRmqProxy(conn, ToxicDirection.DownStream, 0.9, 100),
                conn => { },
                1000, 10);
        }

        [Test]
        public async Task TestRmqTimeoutUpstreamWhileReceiving()
        {
            // tests (n)ack from the client to the rmq
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => { },
                conn => AddTimeoutToRmqProxy(conn, ToxicDirection.UpStream, 0.9, 100),
                1000, 10);
        }

        [Test]
        public async Task TestRmqTimeoutDownstreamWhileReceiving()
        {
            // tests receive call
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => { },
                conn => AddTimeoutToRmqProxy(conn, ToxicDirection.DownStream, 0.9, 100),
                1000, 10);
        }

        [Test]
        public async Task TestRmqTimeoutUpstreamOnFly()
        {
            // tests (n)ack from the client to the rmq
            await _cluster.TestRmqStreamProviderOnFly(
                conn => AddTimeoutToRmqProxy(conn, ToxicDirection.UpStream, 0.9, 100),
                1000, 60);
        }

        [Test]
        public async Task TestRmqTimeoutDownstreamOnFly()
        {
            // tests receive call
            await _cluster.TestRmqStreamProviderOnFly(
                conn => AddTimeoutToRmqProxy(conn, ToxicDirection.DownStream, 0.9, 100),
                1000, 60);
        }

        #endregion

        #region Latency

        [Test]
        public async Task TestRmqLatencyUpstreamWhileSending()
        {
            // tests send call
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => AddLatencyToRmqProxy(conn, ToxicDirection.UpStream, 1.0, 5000, 5000),
                conn => { },
                100, 60);
        }

        [Test]
        public async Task TestRmqLatencyDownstreamWhileSending()
        {
            // tests (n)ack from the rmq to the client
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => AddLatencyToRmqProxy(conn, ToxicDirection.DownStream, 1.0, 5000, 5000),
                conn => { },
                100, 60);
        }

        [Test]
        public async Task TestRmqLatencyUpstreamWhileReceiving()
        {
            // tests (n)ack from the client to the rmq
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => { },
                conn => AddLatencyToRmqProxy(conn, ToxicDirection.UpStream, 1.0, 3000, 3000),
                100, /*60*/int.MaxValue);
        }

        [Test]
        public async Task TestRmqLatencyDownstreamWhileReceiving()
        {
            // tests receive call
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => { },
                conn => AddLatencyToRmqProxy(conn, ToxicDirection.DownStream, 1.0, 3000, 3000),
                100, 60);
        }

        [Test]
        public async Task TestRmqLatencyUpstreamOnFly()
        {
            // tests (n)ack from the client to the rmq
            await _cluster.TestRmqStreamProviderOnFly(
                conn => AddLatencyToRmqProxy(conn, ToxicDirection.UpStream, 1.0, 3000, 3000),
                100, 60);
        }

        [Test]
        public async Task TestRmqLatencyDownstreamOnFly()
        {
            // tests receive call
            await _cluster.TestRmqStreamProviderOnFly(
                conn => AddLatencyToRmqProxy(conn, ToxicDirection.DownStream, 1.0, 3000, 3000),
                100, 60);
        }

        #endregion

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