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

    [ImplicitStreamSubscription(Globals.StreamNameSpaceDefault)]
    [ImplicitStreamSubscription(Globals.StreamNameSpaceProtoBuf)]
    public class ReceiverGrain : Grain, IReceiverGrain
    {
        private ILogger _logger;
        private StreamSubscriptionHandle<Message> _subscriptionDefault;
        private StreamSubscriptionHandle<Message> _subscriptionProtoBuf;

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            _logger = ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger($"{typeof(ReceiverGrain).FullName}.{this.GetPrimaryKey()}");
            _logger.LogInformation($"OnActivateAsync [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}");
            _subscriptionDefault = await GetStreamProvider(Globals.StreamProviderNameDefault)
                .GetStream<Message>(this.GetPrimaryKey(), Globals.StreamNameSpaceDefault)
                .SubscribeAsync(OnNextAsync);
            _subscriptionProtoBuf = await GetStreamProvider(Globals.StreamProviderNameProtoBuf)
                .GetStream<Message>(this.GetPrimaryKey(), Globals.StreamNameSpaceProtoBuf)
                .SubscribeAsync(OnNextAsync);
        }

        public override async Task OnDeactivateAsync()
        {
            _logger.LogInformation($"OnDeactivateAsync [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}");
            await _subscriptionDefault.UnsubscribeAsync();
            await _subscriptionProtoBuf.UnsubscribeAsync();
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