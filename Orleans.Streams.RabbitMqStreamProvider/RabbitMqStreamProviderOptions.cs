using System;
using Orleans.Providers;

namespace Orleans.Streams
{
    public class RabbitMqStreamProviderOptions
    {
        public readonly string HostName;
        public readonly string VirtualHost;
        public readonly int Port;
        public readonly string UserName;
        public readonly string Password;
        public readonly int CacheSize;
        public readonly TimeSpan CacheFillingTimeout;
        public readonly int NumberOfQueues;
        public readonly string QueueNamePrefix;
        
        public RabbitMqStreamProviderOptions(IProviderConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));

            if (!config.Properties.ContainsKey(nameof(UserName))) throw new ArgumentException("Missing required parameter!", nameof(UserName));
            if (!config.Properties.ContainsKey(nameof(Password))) throw new ArgumentException("Missing required parameter!", nameof(Password));
            if (!config.Properties.ContainsKey(nameof(QueueNamePrefix))) throw new ArgumentException("Missing required parameter!", nameof(QueueNamePrefix));

            HostName = config.GetProperty(nameof(HostName), DefaultHostName);
            VirtualHost = config.GetProperty(nameof(VirtualHost), DefaultVirtualHost);
            Port = config.GetIntProperty(nameof(Port), DefaultPort);
            UserName = config.GetProperty(nameof(UserName), string.Empty);
            Password = config.GetProperty(nameof(Password), string.Empty);
            CacheSize = config.GetIntProperty(nameof(CacheSize), DefaultCacheSize);
            CacheFillingTimeout = config.GetTimeSpanProperty(nameof(CacheFillingTimeout), DefaultCacheFillingTimeout);
            QueueNamePrefix = config.GetProperty(nameof(QueueNamePrefix), string.Empty);
            NumberOfQueues = config.GetIntProperty(nameof(NumberOfQueues), DefaultNumberOfQueues);
        }

        private const string DefaultHostName = "localhost";
        private const string DefaultVirtualHost = "/";
        private const int DefaultPort = 5672;
        private const int DefaultCacheSize = 4000;
        private static readonly TimeSpan DefaultCacheFillingTimeout = TimeSpan.FromSeconds(10);
        private const int DefaultNumberOfQueues = 1;
    }
}