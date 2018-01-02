using System;
using System.Threading;
using System.Threading.Tasks;
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
        private StreamSubscriptionHandle<Message> _subscription;

        public override async Task OnActivateAsync()
        {
            GetLogger().Log(0, Orleans.Runtime.Severity.Info, $"OnActivateAsync [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}", null, null);
            await base.OnActivateAsync();
            _subscription = await GetStreamProvider(Globals.StreamProviderName)
                .GetStream<Message>(this.GetPrimaryKey(), Globals.StreamNameSpace)
                .SubscribeAsync(OnNextAsync);
        }

        public override async Task OnDeactivateAsync()
        {
            GetLogger().Log(0, Orleans.Runtime.Severity.Info, $"OnDeactivateAsync [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}", null, null);
            await _subscription.UnsubscribeAsync();
            await base.OnDeactivateAsync();
        }

        private async Task OnNextAsync(Message message, StreamSequenceToken token = null)
        {
            GetLogger().Log(0, Orleans.Runtime.Severity.Info, $"OnNextAsync in #{message.Id} [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}", null, null);
            await Task.Delay(TimeSpan.FromMilliseconds(message.WorkTimeOutMillis));
            await GrainFactory.GetGrain<IAggregatorGrain>(Guid.Empty).MessageReceived(message.CreateDelivered($"[{RuntimeIdentity}],[{IdentityString}]").AsImmutable());
            DeactivateOnIdle();
            GetLogger().Log(0, Orleans.Runtime.Severity.Info, $"OnNextAsync out #{message.Id} [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}", null, null);
        }
    }
}