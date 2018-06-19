using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Streams;
using Orleans.TestingHost;

namespace RabbitMqStreamTests
{
    public static class TestClusterUtils
    {
        public static async Task StartPullingAgents(this TestCluster cluster)
        {
            if (cluster == null) return;
            await cluster
                .GrainFactory
                .GetGrain<IManagementGrain>(0)
                .SendControlCommandToProvider(
                    "Orleans.Streams.RabbitMqStreamProvider",
                    Globals.StreamProviderName,
                    (int)PersistentStreamProviderCommand.StartAgents);
        }

        public static async Task StopPullingAgents(this TestCluster cluster)
        {
            if (cluster == null) return;
            await cluster
                .GrainFactory
                .GetGrain<IManagementGrain>(0)
                .SendControlCommandToProvider(
                    "Orleans.Streams.RabbitMqStreamProvider",
                    Globals.StreamProviderName,
                    (int)PersistentStreamProviderCommand.StopAgents);
        }

        public static TestClusterOptions CreateClusterOptions()
        {
            var options = new TestClusterOptions(2);

            options.ClusterConfiguration.Globals.UseLivenessGossip = false;
            options.ClusterConfiguration.Globals.RegisterStorageProvider<MemoryStorage>("PubSubStore");
            options.ClusterConfiguration.Globals.RegisterStreamProvider<RabbitMqStreamProvider>(Globals.StreamProviderName,
                new Dictionary<string, string>
                {
                    { "HostName", "localhost" },
                    { "Port", ToxiProxyHelpers.RmqProxyPort.ToString() },
                    { "VirtualHost", "/" },
                    { "UserName", "guest" },
                    { "Password", "guest" },
                    { "GetQueueMessagesTimerPeriod", "100ms" },
                    { "UseQueuePartitioning", "false" },
                    { "QueueNamePrefix", "test" },
                    { "NumberOfQueues", "1" },
                    { "CacheSize", "100" },
                    { PersistentStreamProviderConfig.STREAM_PUBSUB_TYPE, StreamPubSubType.ImplicitOnly.ToString() }
                });

            return options;
        }

        public static TestCluster CreateTestCluster(this TestClusterOptions options)
        {
            var cluster = new TestCluster(options);
            cluster.Deploy();
            cluster.WaitForLivenessToStabilizeAsync().Wait();
            return cluster;
        }

        public static void Shutdown(this TestCluster cluster)
        {
            if (cluster == null) return;

            /*  Notes:
             *  - in Orleans 1.4.1 (likely also newer), there is an issue that randevous grain can't be reached and the
             *    provider can't be unregistered, because a communication to grains was already shut down in one of the
             *    previous steps in Silo.Terminate; this causes waiting for a timeout (10 mins) while shutting down
             *    the primary silo! this is of course not acceptable in unit tests or in production when upgrading the services
             *  - to solve the issue, there are two options
             *    (1) in case of only implicit subscription, use { "PubSubType", "ImplicitOnly" } in the stream provider settings
             *      - the explanation is that it is not a grain based model so no communication is required
             *    (2) in general case, step the stream provider prior to stopping the silo using the commented code snipped below
             *  - the following piece of code can be used to control provider of any kind; here we stop a stream provider
             */

            /*cluster?
                .GrainFactory
                .GetGrain<IManagementGrain>(0)
                .SendControlCommandToProvider(
                    "Orleans.Streams.RabbitMqStreamProvider",
                    Globals.StreamProviderName,
                    (int) PersistentStreamProviderCommand.StopAgents)
                .Wait();*/

            try
            {
                cluster.StopAllSilos();
            }
            catch (CannotUnloadAppDomainException)
            {
                // this might happen when unloading the silo
            }
        }
    }
}
