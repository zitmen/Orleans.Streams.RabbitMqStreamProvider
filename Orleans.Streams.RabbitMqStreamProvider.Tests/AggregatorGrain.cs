﻿using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
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
        Task<bool> WereAllSentAlsoDelivered(int count);
        Task<int> GetProcessingSilosCount();
        Task<Message[]> GetAllSentMessages();
        Task<Message[]> GetAllReceivedMessages();
    }

    public class AggregatorGrain : Grain, IAggregatorGrain
    {
        private ILogger _logger;
        private Dictionary<int, Message> _sentMessages;
        private Dictionary<int, Message> _receivedMessages;

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            _logger = ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger($"{typeof(AggregatorGrain).FullName}.{this.GetPrimaryKey()}");
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
            _logger.LogInformation($"MessageSent #{message.Value.Id} [{RuntimeIdentity}],[{IdentityString}] from thread {Thread.CurrentThread.Name}");
            if (_sentMessages.TryGetValue(message.Value.Id, out var value))
                value.Count++;
            else
                _sentMessages.Add(message.Value.Id, message.Value);
            return Task.CompletedTask;
        }

        public Task MessageReceived(Immutable<Message> message)
        {
            _logger.LogInformation($"MessageReceived #{message.Value.Id} [{RuntimeIdentity}],[{IdentityString}] from thread {Thread.CurrentThread.Name}");
            if (_receivedMessages.TryGetValue(message.Value.Id, out var value))
                value.Count++;
            else
                _receivedMessages.Add(message.Value.Id, message.Value);
            return Task.CompletedTask;
        }

        public Task<bool> WereAllMessagesSent(Immutable<Message[]> messages)
        {
            return Task.FromResult(
                messages.Value.Length == _sentMessages.Count &&
                messages.Value.All(msg => _sentMessages.ContainsKey(msg.Id)));
        }

        public Task<bool> WereAllSentAlsoDelivered(int count)
        {
            return Task.FromResult(
                _sentMessages.Count == _receivedMessages.Count &&
                _sentMessages.Values.All(msg => _receivedMessages.TryGetValue(msg.Id, out var message) && message.Delivered && message.Count == count));
        }

        public Task<int> GetProcessingSilosCount()
        {
            return Task.FromResult(_receivedMessages.Values.Select(msg => ExtractRuntimeIdentity(msg.ProcessedBy)).Distinct().Count());
        }

        public Task<Message[]> GetAllSentMessages()
        {
            return Task.FromResult(_sentMessages.Values.ToArray());
        }

        public Task<Message[]> GetAllReceivedMessages()
        {
            return Task.FromResult(_receivedMessages.Values.ToArray());
        }

        private static string ExtractRuntimeIdentity(string processedBy)
        {
            return processedBy.Split(',')[0];
        }
    }
}