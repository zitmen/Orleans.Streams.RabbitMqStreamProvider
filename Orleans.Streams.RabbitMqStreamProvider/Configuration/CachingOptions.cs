using Orleans.Runtime;
using System;

namespace Orleans.Configuration
{
    public class CachingOptions
    {
        public int CacheSize = DefaultCacheSize;
        public TimeSpan CacheFillingTimeout = DefaultCacheFillingTimeout;

        public const int DefaultCacheSize = 4096;
        public static readonly TimeSpan DefaultCacheFillingTimeout = TimeSpan.FromSeconds(10);
    }

    public class CachingOptionsValidator : IConfigurationValidator
    {
        private readonly CachingOptions options;
        private readonly string name;

        public CachingOptionsValidator(CachingOptions options, string name)
        {
            this.options = options;
            this.name = name;
        }

        public void ValidateConfiguration()
        {
            if (options.CacheSize <= 0) ThrowNotPositive(nameof(options.CacheSize));
            if (options.CacheFillingTimeout <= TimeSpan.Zero) ThrowNotPositive(nameof(options.CacheFillingTimeout));
        }

        private void ThrowNotPositive(string parameterName)
            => throw new OrleansConfigurationException($"Value of parameter `{parameterName}` must be positive!");
    }
}
