using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Streams;
using Orleans.Streams.BatchContainer;

namespace Orleans.Streaming
{
    public class SiloRabbitMqStreamConfigurator<TSerializer> : SiloPersistentStreamConfigurator where TSerializer : IBatchContainerSerializer, new()
    {
        public SiloRabbitMqStreamConfigurator(string name, Action<Action<IServiceCollection>> configureDelegate)
            : base(name, configureDelegate, RabbitMqAdapterFactory<TSerializer>.Create)
        {
            this.configureDelegate(services =>
                {
                    services.ConfigureNamedOptionForLogging<RabbitMqOptions>(name)
                        .AddTransient<IConfigurationValidator>(sp => new RabbitMqOptionsValidator(sp.GetOptionsByName<RabbitMqOptions>(name), name))
                        .ConfigureNamedOptionForLogging<CachingOptions>(name)
                        .AddTransient<IConfigurationValidator>(sp => new CachingOptionsValidator(sp.GetOptionsByName<CachingOptions>(name), name));
                });
        }

        public SiloRabbitMqStreamConfigurator<TSerializer> ConfigureRabbitMq(string host, int port, string virtualHost, string user, string password, string queueName, bool useQueuePartitioning = RabbitMqOptions.DefaultUseQueuePartitioning, int numberOfQueues = RabbitMqOptions.DefaultNumberOfQueues)
        {
            Configure<RabbitMqOptions>(ob => ob.Configure(options =>
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
            return this;
        }

        public SiloRabbitMqStreamConfigurator<TSerializer> ConfigureCache(int cacheSize)
        {
            Configure<CachingOptions>(ob => ob.Configure(options => options.CacheSize = cacheSize));
            return this;
        }

        public SiloRabbitMqStreamConfigurator<TSerializer> ConfigureCache(int cacheSize, TimeSpan cacheFillingTimeout)
        {
            Configure<CachingOptions>(ob => ob.Configure(options =>
                {
                    options.CacheSize = cacheSize;
                    options.CacheFillingTimeout = cacheFillingTimeout;
                }));
            return this;
        }
    }

    public class ClusterClientRabbitMqStreamConfigurator<TSerializer> : ClusterClientPersistentStreamConfigurator where TSerializer : IBatchContainerSerializer, new()
    {
        public ClusterClientRabbitMqStreamConfigurator(string name, IClientBuilder builder)
            : base(name, builder, RabbitMqAdapterFactory<TSerializer>.Create)
        {
            clientBuilder
                .ConfigureApplicationParts(parts => parts.AddFrameworkPart(typeof(RabbitMqAdapterFactory<TSerializer>).Assembly))
                .ConfigureServices(services => services
                    .ConfigureNamedOptionForLogging<RabbitMqOptions>(name)
                    .AddTransient<IConfigurationValidator>(sp => new RabbitMqOptionsValidator(sp.GetOptionsByName<RabbitMqOptions>(name), name))
                    .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name));

        }

        public ClusterClientRabbitMqStreamConfigurator<TSerializer> ConfigureRabbitMq(
            string host, int port, string virtualHost, string user, string password, string queueName,
            bool useQueuePartitioning = RabbitMqOptions.DefaultUseQueuePartitioning,
            int numberOfQueues = RabbitMqOptions.DefaultNumberOfQueues)
        {
            Configure<RabbitMqOptions>(ob => ob.Configure(options =>
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
            return this;
        }
    }
}