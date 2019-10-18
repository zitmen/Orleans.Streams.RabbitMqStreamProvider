using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Concurrency;
using Toxiproxy.Net;

namespace RabbitMqStreamTests
{
    internal static class IntegrationTestHelpers
    {
        public static async Task TestRmqStreamProviderWithPrefilledQueue(this TestCluster cluster, Action<Connection> setupProxyForSender, Action<Connection> setupProxyForReceiver, int nMessages, int itersToWait, RmqSerializer serializer = RmqSerializer.Default)
        {
            await cluster.StopPullingAgents();

            var rand = new Random();
            var messages = Enumerable.Range(1, nMessages).Select(id => new Message(id, rand.Next(1, 5) * 100)).ToArray();

            var aggregator = cluster.GrainFactory.GetGrain<IAggregatorGrain>(Guid.Empty);
            await aggregator.CleanUp(); // has to be done here, because the sender is also accessing the aggregator

            using (var connection = new Connection(resetAllToxicsAndProxiesOnClose: true))
            {
                setupProxyForSender(connection);

                var sender = cluster.GrainFactory.GetGrain<ISenderGrain>(Guid.Empty);
                await Task.WhenAll(messages.Select(msg => sender.SendMessage(msg.AsImmutable(), serializer)));
            }

            using (var connection = new Connection(resetAllToxicsAndProxiesOnClose: true))
            {
                setupProxyForReceiver(connection);

                await cluster.StartPullingAgents();

                int iters = 0;
                while (!await AllMessagesSentAndDelivered(aggregator, messages) && iters < itersToWait)
                {
                    iters++;
                    await Task.Delay(TimeSpan.FromSeconds(10));
                }

                Assert.IsTrue(await AllMessagesSentAndDelivered(aggregator, messages), await PrintError(aggregator, messages));
                Assert.AreEqual(cluster.Silos.Count(), await aggregator.GetProcessingSilosCount(), "Silo count mismatch!");
            }
        }

        public static async Task TestRmqStreamProviderOnFly(this TestCluster cluster, Action<Connection> setupProxy, int nMessages, int itersToWait, RmqSerializer serializer = RmqSerializer.Default)
        {
            var rand = new Random();
            var messages = Enumerable.Range(1, nMessages).Select(id => new Message(id, rand.Next(1, 5) * 100)).ToArray();

            var aggregator = cluster.GrainFactory.GetGrain<IAggregatorGrain>(Guid.Empty);
            await aggregator.CleanUp(); // has to be done here, because the sender is also accessing the aggregator

            using (var connection = new Connection(resetAllToxicsAndProxiesOnClose: true))
            {
                setupProxy(connection);

                var sender = cluster.GrainFactory.GetGrain<ISenderGrain>(Guid.Empty);
                await Task.WhenAll(messages.Select(msg => sender.SendMessage(msg.AsImmutable(), serializer)));
            
                int iters = 0;
                while (!await AllMessagesSentAndDelivered(aggregator, messages) && iters < itersToWait)
                {
                    iters++;
                    await Task.Delay(TimeSpan.FromSeconds(10));
                }

                Assert.IsTrue(await AllMessagesSentAndDelivered(aggregator, messages), await PrintError(aggregator, messages));
                Assert.AreEqual(cluster.Silos.Count(), await aggregator.GetProcessingSilosCount(), "Silo count mismatch!");
            }
        }

        private static async Task<bool> AllMessagesSentAndDelivered(IAggregatorGrain aggregator, Message[] messages)
            => await aggregator.WereAllMessagesSent(messages.AsImmutable()) &&
               await aggregator.WereAllSentAlsoDelivered();

        private static async Task<string> PrintError(IAggregatorGrain aggregator, Message[] messages)
        {
            var sb = new StringBuilder();
            sb.AppendLine("Expectation failed!");
            sb.AppendLine($" -expected: {string.Join(',', messages.OrderBy(m => m.Id).Select(m => m.Id))}");
            sb.AppendLine($" -sent    : {string.Join(',', (await aggregator.GetAllSentMessages()).OrderBy(m => m.Id).Select(m => m.Id))}");
            sb.AppendLine($" -received: {string.Join(',', (await aggregator.GetAllReceivedMessages()).OrderBy(m => m.Id).Select(m => m.Id))}");
            return sb.ToString();
        }
    }
}