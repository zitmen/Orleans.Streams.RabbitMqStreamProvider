using Orleans.Runtime;

namespace Orleans.Configuration
{
    public class RabbitMqOptions
    {
        public string HostName;
        public int Port;
        public string VirtualHost;
        public string UserName;
        public string Password;

        public string QueueNamePrefix;
        public bool UseQueuePartitioning = DefaultUseQueuePartitioning;
        public int NumberOfQueues = DefaultNumberOfQueues;

        
        public const bool DefaultUseQueuePartitioning = false;
        public const int DefaultNumberOfQueues = 1;
    }

    public class RabbitMqOptionsValidator : IConfigurationValidator
    {
        private readonly RabbitMqOptions options;
        private readonly string name;

        public RabbitMqOptionsValidator(RabbitMqOptions options, string name)
        {
            this.options = options;
            this.name = name;
        }

        public void ValidateConfiguration()
        {
            if (string.IsNullOrEmpty(options.HostName)) ThrowMissing(nameof(options.HostName));
            if (string.IsNullOrEmpty(options.VirtualHost)) ThrowMissing(nameof(options.VirtualHost));
            if (string.IsNullOrEmpty(options.UserName)) ThrowMissing(nameof(options.UserName));
            if (string.IsNullOrEmpty(options.Password)) ThrowMissing(nameof(options.Password));
            if (string.IsNullOrEmpty(options.QueueNamePrefix)) ThrowMissing(nameof(options.QueueNamePrefix));
            if (options.Port <= 0) ThrowNotPositive(nameof(options.Port));
            if (options.UseQueuePartitioning && options.NumberOfQueues <= 0) ThrowNotPositive(nameof(options.NumberOfQueues));
        }

        private void ThrowMissing(string parameterName)
            => throw new OrleansConfigurationException($"Missing required parameter `{parameterName}` on stream provider {name}!");

        private void ThrowNotPositive(string parameterName)
            => throw new OrleansConfigurationException($"Value of parameter `{parameterName}` must be positive!");
    }
}