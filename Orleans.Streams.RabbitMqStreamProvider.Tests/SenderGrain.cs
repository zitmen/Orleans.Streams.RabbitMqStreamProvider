using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace RabbitMqStreamTests
{
    public interface ISenderGrain : IGrainWithGuidKey
    {
        Task SendMessage(Immutable<Message> message);
    }

    [StatelessWorker]
    public class SenderGrain : Grain, ISenderGrain
    {
        public async Task SendMessage(Immutable<Message> message)
        {
            GetLogger().Log(0, Orleans.Runtime.Severity.Info, $"SendMessage #{message.Value.Id} [{RuntimeIdentity}],[{IdentityString}]", null, null);

            await GetStreamProvider(Globals.StreamProviderName)
                .GetStream<Message>(Guid.NewGuid(), Globals.StreamNameSpace)
                .OnNextAsync(message.Value);

            await GrainFactory.GetGrain<IAggregatorGrain>(Guid.Empty).MessageSent(message);
        }
    }
}