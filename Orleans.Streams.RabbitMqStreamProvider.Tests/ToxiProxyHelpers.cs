using System.Diagnostics;
using Toxiproxy.Net;
using Toxiproxy.Net.Toxics;

namespace RabbitMqStreamTests
{
    internal static class ToxiProxyHelpers
    {
        private const string RmqProxyName = "RMQ";
        private const int RmqPort = 5672;
        public const int RmqProxyPort = 56720;

        public static Process StartProxy()
        {
            StopProxyIfRunning();

            var proxyProcess = new Process
            {
                StartInfo = new ProcessStartInfo(@"..\..\..\packages\Toxiproxy.Net.2.0.1\compiled\Win64\toxiproxy-server-2.1.2-windows-amd64.exe")
            };
            proxyProcess.Start();

            new Connection().Client().Add(new Proxy
            {
                Name = RmqProxyName,
                Enabled = true,
                Listen = $"localhost:{RmqProxyPort}",
                Upstream = $"orlytest.golamago.online:{RmqPort}"
            });

            return proxyProcess;
        }

        public static void StopProxyIfRunning()
        {
            foreach (var process in Process.GetProcessesByName("toxiproxy-server-2.1.2-windows-amd64"))
            {
                process.CloseMainWindow();
                process.WaitForExit();
            }
        }

        public static void AddTimeoutToRmqProxy(Connection connection, ToxicDirection direction, double toxicity, int timeout)
        {
            var proxy = connection.Client().FindProxy(RmqProxyName);
            proxy.Add(new TimeoutToxic
            {
                Name = "Timeout",
                Toxicity = toxicity,
                Stream = direction,
                Attributes = new TimeoutToxic.ToxicAttributes
                {
                    Timeout = timeout
                }
            });
            proxy.Update();
        }

        public static void AddLatencyToRmqProxy(Connection connection, ToxicDirection direction, double toxicity, int latency, int jitter)
        {
            var proxy = connection.Client().FindProxy(RmqProxyName);
            proxy.Add(new LatencyToxic
            {
                Name = "Latency",
                Toxicity = toxicity,
                Stream = direction,
                Attributes = new LatencyToxic.ToxicAttributes
                {
                    Latency = latency,
                    Jitter = jitter
                }
            });
            proxy.Update();
        }
    }
}