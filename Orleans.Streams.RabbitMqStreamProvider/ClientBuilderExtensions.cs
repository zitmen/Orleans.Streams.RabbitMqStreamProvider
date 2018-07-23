using System;
using Orleans.Streaming;
using Orleans.Streams.BatchContainer;

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

        /// <summary>
        /// Configure client to use RMQ persistent streams.
        /// This version uses the default Orleans serializer.
        /// </summary>
        public static IClientBuilder AddRabbitMqStream(this IClientBuilder builder, string name, Action<ClusterClientRabbitMqStreamConfigurator<DefaultBatchContainerSerializer>> configure)
            => AddRabbitMqStream<DefaultBatchContainerSerializer>(builder, name, configure);
    }
}