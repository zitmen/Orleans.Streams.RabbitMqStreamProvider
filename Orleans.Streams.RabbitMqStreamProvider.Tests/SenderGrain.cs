using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Concurrency;
using Orleans.Streams.RabbitMq;

namespace RabbitMqStreamTests
{
    public interface ISenderGrain : IGrainWithGuidKey
    {
        Task SendMessage(Immutable<Message> message);
    }

    [StatelessWorker]
    public class SenderGrain : Grain, ISenderGrain
    {
        private ILogger _logger;

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            _logger = ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger($"{typeof(SenderGrain).FullName}.{this.GetPrimaryKey()}");
        }

        public async Task SendMessage(Immutable<Message> message)
        {
            _logger.LogInformation($"SendMessage #{message.Value.Id} [{RuntimeIdentity}],[{IdentityString}] from thread {Thread.CurrentThread.Name}");

            while (true)
            {
                try
                {
                    await GetStreamProvider(Globals.StreamProviderName)
                        .GetStream<Message>(Guid.NewGuid(), Globals.StreamNameSpace)
                        .OnNextAsync(message.Value);
                    break;
                }
                catch (RabbitMqException ex)
                {
                    _logger.LogError(ex, $"SendMessage #{message.Value.Id} [{RuntimeIdentity}],[{IdentityString}] from thread {Thread.CurrentThread.Name} failed!");
                }
                await Task.Delay(TimeSpan.FromMilliseconds(100));
            }

            await GrainFactory.GetGrain<IAggregatorGrain>(Guid.Empty).MessageSent(message);
        }
    }
}