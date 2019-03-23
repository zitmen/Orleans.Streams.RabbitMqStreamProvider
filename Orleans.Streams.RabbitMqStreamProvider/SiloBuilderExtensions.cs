using System;
using Orleans.Streaming;
using Orleans.Streams.BatchContainer;

namespace Orleans.Hosting
{
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use RMQ persistent streams.
        /// This version enables to inject a custom BacthContainer serializer.
        /// </summary>
        public static ISiloHostBuilder AddRabbitMqStream<TSerializer>(this ISiloHostBuilder builder, string name, Action<SiloRabbitMqStreamConfigurator<TSerializer>> configure) where TSerializer : IBatchContainerSerializer, new()
        {
            configure?.Invoke(new SiloRabbitMqStreamConfigurator<TSerializer>(name, configDelegate => builder.ConfigureServices(configDelegate), builder));
            return builder;
        }

        /// <summary>
        /// Configure silo to use RMQ persistent streams.
        /// This version uses the default Orleans serializer.
        /// </summary>
        public static ISiloHostBuilder AddRabbitMqStream(this ISiloHostBuilder builder, string name, Action<SiloRabbitMqStreamConfigurator<DefaultBatchContainerSerializer>> configure)
            => AddRabbitMqStream<DefaultBatchContainerSerializer>(builder, name, configure);
    }
}