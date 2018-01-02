using System;
using System.Threading;
using System.Threading.Tasks;
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
        public async Task SendMessage(Immutable<Message> message)
        {
            GetLogger().Log(0, Orleans.Runtime.Severity.Info, $"SendMessage #{message.Value.Id} [{RuntimeIdentity}],[{IdentityString}] from thread {Thread.CurrentThread.Name}", null, null);

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
                    GetLogger().Log(0, Orleans.Runtime.Severity.Error, $"SendMessage #{message.Value.Id} [{RuntimeIdentity}],[{IdentityString}] from thread {Thread.CurrentThread.Name} failed!", null, ex);
                }
                await Task.Delay(TimeSpan.FromMilliseconds(100));
            }

            await GrainFactory.GetGrain<IAggregatorGrain>(Guid.Empty).MessageSent(message);
        }
    }
}