using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Concurrency;
using Orleans.Streams;

namespace RabbitMqStreamTests
{
    public interface IReceiverGrain : IGrainWithGuidKey
    {
    }

    [ImplicitStreamSubscription(Globals.StreamNameSpace)]
    public class ReceiverGrain : Grain, IReceiverGrain
    {
        private ILogger _logger;
        private StreamSubscriptionHandle<Message> _subscription;

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            _logger = ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger($"{typeof(ReceiverGrain).FullName}.{this.GetPrimaryKey()}");
            _logger.LogInformation($"OnActivateAsync [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}");
            _subscription = await GetStreamProvider(Globals.StreamProviderName)
                .GetStream<Message>(this.GetPrimaryKey(), Globals.StreamNameSpace)
                .SubscribeAsync(OnNextAsync);
        }

        public override async Task OnDeactivateAsync()
        {
            _logger.LogInformation($"OnDeactivateAsync [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}");
            await _subscription.UnsubscribeAsync();
            await base.OnDeactivateAsync();
        }

        private async Task OnNextAsync(Message message, StreamSequenceToken token = null)
        {
            _logger.LogInformation($"OnNextAsync in #{message.Id} [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}");
            await Task.Delay(TimeSpan.FromMilliseconds(message.WorkTimeOutMillis));
            await GrainFactory.GetGrain<IAggregatorGrain>(Guid.Empty).MessageReceived(message.CreateDelivered($"[{RuntimeIdentity}],[{IdentityString}]").AsImmutable());
            DeactivateOnIdle();
            _logger.LogInformation($"OnNextAsync out #{message.Id} [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}");
        }
    }
}