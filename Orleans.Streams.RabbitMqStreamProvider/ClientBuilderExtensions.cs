using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Streams;
using Orleans.Streams.BatchContainer;
using System;

namespace Orleans.Hosting
{
    public static class ClientBuilderExtensions
    {
        /// <summary>
        /// Configure client to use RMQ persistent streams.
        /// This version enables to inject a custom BacthContainer serializer.
        /// </summary>
        public static IClientBuilder AddRabbitMqStream<TSerializer>(this IClientBuilder builder, string name, Action<ClusterClientRabbitMqStreamConfigurator<TSerializer>> configure) where TSerializer : IBatchContainerSerializer, new()
        {
            configure?.Invoke(new ClusterClientRabbitMqStreamConfigurator<TSerializer>(name, builder));
            return builder;
        }

        public class ClusterClientRabbitMqStreamConfigurator<TSerializer> : ClusterClientPersistentStreamConfigurator, IClusterClientAzureQueueStreamConfigurator where TSerializer : IBatchContainerSerializer, new()
        {
            public ClusterClientRabbitMqStreamConfigurator(string name, IClientBuilder builder)
                : base(name, builder, RabbitMqAdapterFactory<TSerializer>.Create)
            {
                this.ConfigureComponent(SimpleQueueCacheOptionsValidator.Create);

                builder
                    .ConfigureApplicationParts(RabbitMQStreamConfiguratorCommon<TSerializer>.AddParts)
                    .ConfigureServices(services =>
                        services.ConfigureNamedOptionForLogging<RabbitMqOptions>(name)
                            .AddTransient<IConfigurationValidator>(sp => new RabbitMqOptionsValidator(sp.GetOptionsByName<RabbitMqOptions>(name), name))
                            .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name));

            }
        }

        /// <summary>
        /// Configure client to use RMQ persistent streams.
        /// This version uses the default Orleans serializer.
        /// </summary>
        public static IClientBuilder AddRabbitMqStream(this IClientBuilder builder, string name, Action<ClusterClientRabbitMqStreamConfigurator<DefaultBatchContainerSerializer>> configure)
            => AddRabbitMqStream<DefaultBatchContainerSerializer>(builder, name, configure);

        public interface IClusterClientAzureQueueStreamConfigurator : IRabbitMQStreamConfigurator, IClusterClientPersistentStreamConfigurator { }
    }
}