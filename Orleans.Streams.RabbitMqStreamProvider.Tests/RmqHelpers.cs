﻿using RabbitMQ.Client;
using System;

namespace RabbitMqStreamTests
{
    public enum RmqSerializer
    {
        Default,
        ProtoBuf
    }

    public static class RmqHelpers
    {
        public static void EnsureEmptyQueue()
        {
            var factory = new ConnectionFactory
            {
                HostName = "orlytest.golamago.online",
                VirtualHost = "stream-test",
                Port = 5672,
                UserName = "lama-testing",
                Password = "testing"
            };

            try
            {
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    channel.QueuePurge(Globals.StreamNameSpaceDefault);
                    channel.QueuePurge(Globals.StreamNameSpaceProtoBuf);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}
