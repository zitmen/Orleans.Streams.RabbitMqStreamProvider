using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.ApplicationParts;
using Orleans.Configuration;
using Orleans.Streams;
using Orleans.Streams.BatchContainer;
using System;

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

    public interface IRabbitMQStreamConfigurator : INamedServiceConfigurator { }

    public class SiloRabbitMqStreamConfigurator<TSerializer> : SiloPersistentStreamConfigurator, ISiloRabbitMQStreamConfigurator where TSerializer : IBatchContainerSerializer, new()
    {
        public SiloRabbitMqStreamConfigurator(string name, Action<Action<IServiceCollection>> configureDelegate, ISiloHostBuilder builder)
            : base(name, configureDelegate, RabbitMqAdapterFactory<TSerializer>.Create)
        {
            this.ConfigureComponent(SimpleQueueCacheOptionsValidator.Create);
            builder.ConfigureApplicationParts(RabbitMQStreamConfiguratorCommon<TSerializer>.AddParts);

            ConfigureDelegate(services =>
            {
                services
                    .ConfigureNamedOptionForLogging<RabbitMqOptions>(name)
                    .AddTransient<IConfigurationValidator>(sp => new RabbitMqOptionsValidator(sp.GetOptionsByName<RabbitMqOptions>(name), name));
            });
        }
    }

    public static class RabbitMQStreamConfiguratorExtensions
    {
        public static void ConfigureRabbitMQ(this IRabbitMQStreamConfigurator configurator, Action<OptionsBuilder<RabbitMqOptions>> configureOptions)
        {
            configurator.Configure(configureOptions);
        }

        public static void ConfigureRabbitMq(this IRabbitMQStreamConfigurator configurator, string host, int port, string virtualHost, string user, string password, string queueName, bool useQueuePartitioning = RabbitMqOptions.DefaultUseQueuePartitioning, int numberOfQueues = RabbitMqOptions.DefaultNumberOfQueues)
        {
            configurator.Configure<RabbitMqOptions>(ob => ob.Configure(options =>
            {
                options.HostName = host;
                options.Port = port;
                options.VirtualHost = virtualHost;
                options.UserName = user;
                options.Password = password;
                options.QueueNamePrefix = queueName;
                options.UseQueuePartitioning = useQueuePartitioning;
                options.NumberOfQueues = numberOfQueues;
            }));
        }

        public static void ConfigureCache(this IRabbitMQStreamConfigurator configurator, int cacheSize) =>
            configurator.Configure<CachingOptions>(ob => ob.Configure(options => options.CacheSize = cacheSize));

        public static void ConfigureCache(this IRabbitMQStreamConfigurator configurator, int cacheSize, TimeSpan cacheFillingTimeout) =>
            configurator.Configure<CachingOptions>(ob => ob.Configure(options =>
            {
                options.CacheSize = cacheSize;
                options.CacheFillingTimeout = cacheFillingTimeout;
            }));
    }

    public interface ISiloRabbitMQStreamConfigurator : IRabbitMQStreamConfigurator, ISiloPersistentStreamConfigurator { }

    public static class RabbitMQStreamConfiguratorCommon<TSerializer> where TSerializer : IBatchContainerSerializer, new()
    {
        public static void AddParts(IApplicationPartManager parts)
        {
            parts.AddFrameworkPart(typeof(RabbitMqAdapterFactory<TSerializer>).Assembly);
        }
    }
}