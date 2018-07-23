# Orleans.Streams.RabbitMqStreamProvider
Orleans persistent stream provider for RabbitMQ. This provider is reliable, thus ACKing messages only after they are processed by a consumer.

[![Build status](https://ci.appveyor.com/api/projects/status/vt48gs9gtghynyit?svg=true)](https://ci.appveyor.com/project/zitmen65687/orleans-streams-rabbitmqstreamprovider)
[![NuGet version](https://badge.fury.io/nu/Orleans.Streams.RabbitMqStreamProvider.svg)](https://badge.fury.io/nu/Orleans.Streams.RabbitMqStreamProvider)

Example configuration (see `TestCluster.Create()` in `Orleans.Streams.RabbitMqStreamProvider.Tests` for more):
```
var silo = new SiloHostBuilder()
    .UseLocalhostClustering()
    .AddMemoryGrainStorage("PubSubStore")
    .AddRabbitMqStream("RMQProvider", configurator =>
    {
        configurator.ConfigureRabbitMq(host: "localhost", port: 5672, virtualHost: "/",
                                       user: "guest", password: "guest", queueName: "test");
    })
    .ConfigureLogging(log => log.AddConsole())
    .Build();

await silo.StartAsync();
```
