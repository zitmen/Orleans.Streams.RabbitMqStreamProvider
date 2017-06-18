# Orleans.Streams.RabbitMqStreamProvider
Orleans persistent stream provider for RabbitMQ. This provider is reliable, thus ACKing messages only after they are processed by a consumer.

[![Build status](https://ci.appveyor.com/api/projects/status/vt48gs9gtghynyit?svg=true)](https://ci.appveyor.com/project/zitmen65687/orleans-streams-rabbitmqstreamprovider)
[![NuGet version](https://badge.fury.io/nu/Orleans.Streams.RabbitMqStreamProvider.svg)](https://badge.fury.io/nu/Orleans.Streams.RabbitMqStreamProvider)

Example configuration (see `RabbitMqStreamProviderOptions.cs` for details):
```
<Provider Type="Orleans.Streams.RabbitMqStreamProvider"
          Name="RMQProvider"
          GetQueueMessagesTimerPeriod="1s"
          HostName="localhost"
          Port="5672"
          VirtualHost="/"
          UserName="guest"
          Password="guest"
          QueueNamePrefix="test" />
```
