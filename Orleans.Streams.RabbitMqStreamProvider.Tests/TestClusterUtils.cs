using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Streams.BatchContainer;
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
                    typeof(PersistentStreamProvider).FullName,
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
                    typeof(PersistentStreamProvider).FullName,
                    Globals.StreamProviderName,
                    (int)PersistentStreamProviderCommand.StopAgents);
        }

        public static TestCluster CreateTestCluster(RmqSerializer serializer)
        {
            var builder = new TestClusterBuilder(initialSilosCount: 1);
            switch (serializer)
            {
                case RmqSerializer.ProtoBuf:
                    builder.AddSiloBuilderConfigurator<SiloTestClusterRmqCustomSerializerConfigurator<ProtoBufBatchContainerSerializer>>();
                    break;

                default:
                    builder.AddSiloBuilderConfigurator<SiloTestClusterRmqConfigurator>();
                    break;
            }

            var cluster = builder.Build();
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
             *    (2) in general case, stop the stream provider prior to stopping the silo using the commented code snipped below
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

        public enum RmqSerializer
        {
            Default,
            ProtoBuf
        }

        private class SiloTestClusterRmqConfigurator : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder builder)
            {
                builder.AddMemoryGrainStorage("PubSubStore");

                builder.AddRabbitMqStream(Globals.StreamProviderName, configurator =>
                {
                    configurator.ConfigureRabbitMq(host: "localhost", port: ToxiProxyHelpers.RmqProxyPort,
                        virtualHost: "/", user: "guest", password: "guest", queueName: "test");
                    configurator.ConfigureCache(cacheSize: 100, cacheFillingTimeout: TimeSpan.FromSeconds(10));
                    configurator.ConfigureStreamPubSub(StreamPubSubType.ImplicitOnly);
                    configurator.ConfigurePullingAgent(ob => ob.Configure(
                        options =>
                        {
                            options.GetQueueMsgsTimerPeriod = TimeSpan.FromMilliseconds(100);
                        }));
                });

                builder.ConfigureLogging(log => log.AddConsole());
            }
        }

        private class SiloTestClusterRmqCustomSerializerConfigurator<TSerializer> : ISiloBuilderConfigurator where TSerializer : IBatchContainerSerializer, new()
        {
            public void Configure(ISiloHostBuilder builder)
            {
                builder.AddMemoryGrainStorage("PubSubStore");

                builder.AddRabbitMqStream<TSerializer>(Globals.StreamProviderName, configurator =>
                {
                    configurator.ConfigureRabbitMq(host: "localhost", port: ToxiProxyHelpers.RmqProxyPort,
                        virtualHost: "/", user: "guest", password: "guest", queueName: "test");
                    configurator.ConfigureCache(cacheSize: 100, cacheFillingTimeout: TimeSpan.FromSeconds(10));
                    configurator.ConfigureStreamPubSub(StreamPubSubType.ImplicitOnly);
                    configurator.ConfigurePullingAgent(ob => ob.Configure(
                        options =>
                        {
                            options.GetQueueMsgsTimerPeriod = TimeSpan.FromMilliseconds(100);
                        }));
                });

                builder.ConfigureLogging(log => log.AddConsole());
            }
        }
    }
}