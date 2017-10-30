using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace RabbitMqStreamTests
{
    public interface IAggregatorGrain : IGrainWithGuidKey
    {
        Task CleanUp();
        Task MessageSent(Immutable<Message> message);
        Task MessageReceived(Immutable<Message> message);
        Task<bool> WereAllMessagesSent(Immutable<Message[]> messages);
        Task<bool> WereAllSentAlsoDelivered();
        Task<int> GetProcessingSilosCount();
    }

    public class AggregatorGrain : Grain, IAggregatorGrain
    {
        private Dictionary<int, Message> _sentMessages;
        private Dictionary<int, Message> _receivedMessages;

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            _sentMessages = new Dictionary<int, Message>();
            _receivedMessages = new Dictionary<int, Message>();
        }

        public Task CleanUp()
        {
            _sentMessages.Clear();
            _receivedMessages.Clear();
            return Task.CompletedTask;
        }

        public Task MessageSent(Immutable<Message> message)
        {
            GetLogger().Log(0, Orleans.Runtime.Severity.Info, $"MessageSent #{message.Value.Id} [{RuntimeIdentity}],[{IdentityString}]", null, null);
            _sentMessages.Add(message.Value.Id, message.Value);
            return Task.CompletedTask;
        }

        public Task MessageReceived(Immutable<Message> message)
        {
            GetLogger().Log(0, Orleans.Runtime.Severity.Info, $"MessageReceived #{message.Value.Id} [{RuntimeIdentity}],[{IdentityString}]", null, null);
            _receivedMessages.Add(message.Value.Id, message.Value);
            return Task.CompletedTask;
        }

        public Task<bool> WereAllMessagesSent(Immutable<Message[]> messages)
        {
            return Task.FromResult(
                messages.Value.Length == _sentMessages.Count &&
                messages.Value.All(msg => _sentMessages.ContainsKey(msg.Id)));
        }

        public Task<bool> WereAllSentAlsoDelivered()
        {
            return Task.FromResult(
                _sentMessages.Count == _receivedMessages.Count &&
                _sentMessages.Values.All(msg => _receivedMessages.ContainsKey(msg.Id) && _receivedMessages[msg.Id].Delivered));
        }

        public Task<int> GetProcessingSilosCount()
        {
            return Task.FromResult(_receivedMessages.Values.Select(msg => ExtractRuntimeIdentity(msg.ProcessedBy)).Distinct().Count());
        }

        private static string ExtractRuntimeIdentity(string processedBy)
        {
            return processedBy.Split(',')[0];
        }
    }
}