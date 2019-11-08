using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace RabbitMqStreamTests
{
    public class TestCluster
    {
        private readonly ISiloHost _primarySilo;
        private readonly ISiloHost _secondarySilo;
        private readonly IClusterClient _client;

        private TestCluster(ISiloHost primarySilo, ISiloHost secondarySilo, IClusterClient client)
        {
            _primarySilo = primarySilo;
            _secondarySilo = secondarySilo;
            _client = client;
        }

        public ISiloHost[] Silos => new[] { _primarySilo, _secondarySilo };
        public IGrainFactory GrainFactory => _client;

        public async Task StartPullingAgents()
        {
            await _client
                .GetGrain<IManagementGrain>(0)
                .SendControlCommandToProvider(
                    typeof(PersistentStreamProvider).FullName,
                    Globals.StreamProviderNameDefault,
                    (int)PersistentStreamProviderCommand.StartAgents);

            await _client
                .GetGrain<IManagementGrain>(0)
                .SendControlCommandToProvider(
                    typeof(PersistentStreamProvider).FullName,
                    Globals.StreamProviderNameProtoBuf,
                    (int)PersistentStreamProviderCommand.StartAgents);
        }

        public async Task StopPullingAgents()
        {
            await _client
                .GetGrain<IManagementGrain>(0)
                .SendControlCommandToProvider(
                    typeof(PersistentStreamProvider).FullName,
                    Globals.StreamProviderNameDefault,
                    (int)PersistentStreamProviderCommand.StopAgents);

            await _client
                .GetGrain<IManagementGrain>(0)
                .SendControlCommandToProvider(
                    typeof(PersistentStreamProvider).FullName,
                    Globals.StreamProviderNameProtoBuf,
                    (int)PersistentStreamProviderCommand.StopAgents);
        }

        public static async Task<TestCluster> Create()
        {
            var primarySilo = new SiloHostBuilder()
                .UseLocalhostClustering(siloPort: 11111, gatewayPort: 30000)
                .Configure<ClusterMembershipOptions>(options =>
                {
                    options.UseLivenessGossip = true;
                    options.ProbeTimeout = TimeSpan.FromSeconds(5);
                    options.NumMissedProbesLimit = 3;
                })
                .ConfigureStreamsAndLogging()
                .Build();

            var secondarySilo = new SiloHostBuilder()
                .UseLocalhostClustering(siloPort: 11112, gatewayPort: 30001,
                    primarySiloEndpoint: new IPEndPoint(IPAddress.Loopback, EndpointOptions.DEFAULT_SILO_PORT))
                .Configure<ClusterMembershipOptions>(options =>
                {
                    options.UseLivenessGossip = true;
                    options.ProbeTimeout = TimeSpan.FromSeconds(5);
                    options.NumMissedProbesLimit = 3;
                })
                .ConfigureStreamsAndLogging()
                .Build();

            var client = new ClientBuilder()
                .UseLocalhostClustering(gatewayPorts: new[] { 30000, 30001 })
                .ConfigureStreamsAndLogging()
                .Build();
            
            await primarySilo.StartAsync();
            await secondarySilo.StartAsync();

            // wait for the cluster to stabilize; otherwise only one silo could send & process all messages
            await Task.Delay(TimeSpan.FromMinutes(1));

            await client.Connect();

            return new TestCluster(primarySilo, secondarySilo, client);
        }

        public async Task Shutdown()
        {
            await _client.Close();
            await _secondarySilo.StopAsync();
            await _primarySilo.StopAsync();
        }
    }

    internal static class ClusterBuilderExtentions
    {
        public static ISiloHostBuilder ConfigureStreamsAndLogging(this ISiloHostBuilder builder)
        {
            return builder
                .AddMemoryGrainStorage("PubSubStore")
                .AddRabbitMqStream(Globals.StreamProviderNameDefault, configurator =>
                {
                    configurator.ConfigureRabbitMq(host: "localhost", port: ToxiProxyHelpers.RmqProxyPort,
                        virtualHost: "/", user: "guest", password: "guest", queueName: Globals.StreamNameSpaceDefault);
                    configurator.ConfigureCache(cacheSize: 100, cacheFillingTimeout: TimeSpan.FromSeconds(10));
                    configurator.ConfigureStreamPubSub(StreamPubSubType.ImplicitOnly);
                    configurator.ConfigurePullingAgent(ob => ob.Configure(
                        options =>
                        {
                            options.GetQueueMsgsTimerPeriod = TimeSpan.FromMilliseconds(100);
                        }));
                })
                .AddRabbitMqStream<ProtoBufBatchContainerSerializer>(Globals.StreamProviderNameProtoBuf, configurator =>
                {
                    configurator.ConfigureRabbitMq(host: "localhost", port: ToxiProxyHelpers.RmqProxyPort,
                        virtualHost: "/", user: "guest", password: "guest", queueName: Globals.StreamNameSpaceProtoBuf);
                    configurator.ConfigureCache(cacheSize: 100, cacheFillingTimeout: TimeSpan.FromSeconds(10));
                    configurator.ConfigureStreamPubSub(StreamPubSubType.ImplicitOnly);
                    configurator.ConfigurePullingAgent(ob => ob.Configure(
                        options =>
                        {
                            options.GetQueueMsgsTimerPeriod = TimeSpan.FromMilliseconds(100);
                        }));
                })
                .ConfigureLogging(log => log
                    .ClearProviders()
                    .SetMinimumLevel(LogLevel.Trace)
                    .AddConsole()
                    .AddDebug());
        }

        public static IClientBuilder ConfigureStreamsAndLogging(this IClientBuilder builder)
        {
            return builder
                .AddRabbitMqStream(Globals.StreamProviderNameDefault, configurator =>
                {
                    configurator.ConfigureRabbitMq(host: "localhost", port: ToxiProxyHelpers.RmqProxyPort,
                        virtualHost: "/", user: "guest", password: "guest", queueName: Globals.StreamNameSpaceDefault);
                })
                .AddRabbitMqStream<ProtoBufBatchContainerSerializer>(Globals.StreamProviderNameProtoBuf, configurator =>
                {
                    configurator.ConfigureRabbitMq(host: "localhost", port: ToxiProxyHelpers.RmqProxyPort,
                        virtualHost: "/", user: "guest", password: "guest", queueName: Globals.StreamNameSpaceProtoBuf);
                })
                .ConfigureLogging(log => log
                    .ClearProviders()
                    .SetMinimumLevel(LogLevel.Trace)
                    .AddConsole()
                    .AddDebug());
        }
    }
}