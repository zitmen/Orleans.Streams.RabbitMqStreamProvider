using System;
using System.Threading;
using Orleans.Runtime;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Orleans.Streams.RabbitMq
{
    internal class RabbitMqConsumer : IRabbitMqConsumer
    {
        private readonly RabbitMqConnector _connection;

        public RabbitMqConsumer(RabbitMqConnector connection)
        {
            _connection = connection;
        }

        public void Dispose()
        {
            _connection.Dispose();
        }

        public void Ack(ulong deliveryTag)
        {
            try
            {
                _connection.Logger.Verbose(0, $"RabbitMqConsumer: calling Ack on thread {Thread.CurrentThread.Name}.");

                _connection.Channel.BasicAck(deliveryTag, false);
            }
            catch (Exception ex)
            {
                _connection.Logger.Error(0, "RabbitMqConsumer: failed to call ACK!", ex);
            }
        }

        public void Nack(ulong deliveryTag)
        {
            try
            {
                _connection.Logger.Verbose(0, $"RabbitMqConsumer: calling Nack on thread {Thread.CurrentThread.Name}.");

                _connection.Channel.BasicNack(deliveryTag, false, true);
            }
            catch (Exception ex)
            {
                _connection.Logger.Error(0, "RabbitMqConsumer: failed to call NACK!", ex);
            }
        }

        public BasicGetResult Receive()
        {
            try
            {
                return _connection.Channel.BasicGet(_connection.QueueName, false);
            }
            catch (Exception ex)
            {
                _connection.Logger.Error(0, "RabbitMqConsumer: failed to call Get!", ex);
                return null;
            }
        }
    }

    internal class RabbitMqProducer : IRabbitMqProducer
    {
        private readonly RabbitMqConnector _connection;

        public RabbitMqProducer(RabbitMqConnector connection)
        {
            _connection = connection;
        }

        public void Dispose()
        {
            _connection.Dispose();
        }

        public void Send(byte[] message)
        {
            try
            {
                _connection.Logger.Verbose(0, $"RabbitMqProducer: calling Send on thread {Thread.CurrentThread.Name}.");

                var basicProperties = _connection.Channel.CreateBasicProperties();
                basicProperties.MessageId = Guid.NewGuid().ToString();
                basicProperties.DeliveryMode = 2;   // persistent

                _connection.Channel.BasicPublish(string.Empty, _connection.QueueName, true, basicProperties, message);

                _connection.Channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
            }
            catch (Exception ex)
            {
                throw new RabbitMqException("RabbitMqProducer: Send failed!", ex);
            }
        }
    }

    internal class RabbitMqConnector : IDisposable
    {
        public readonly string QueueName;
        public readonly Logger Logger;

        private readonly RabbitMqStreamProviderOptions _options;
        private IConnection _connection;
        private IModel _channel;

        public IModel Channel
        {
            get
            {
                EnsureConnection();
                return _channel;
            }
        }

        public RabbitMqConnector(RabbitMqStreamProviderOptions options, QueueId queueId, Logger logger)
        {
            _options = options;
            Logger = logger;
            QueueName = options.UseQueuePartitioning
                ? $"{options.QueueNamePrefix}-{queueId.GetNumericId()}"
                : options.QueueNamePrefix;
        }

        private void EnsureConnection()
        {
            if (_connection?.IsOpen != true)
            {
                Logger.Verbose("Opening a new RMQ connection...");
                var factory = new ConnectionFactory
                {
                    HostName = _options.HostName,
                    VirtualHost = _options.VirtualHost,
                    Port = _options.Port,
                    UserName = _options.UserName,
                    Password = _options.Password,
                    UseBackgroundThreadsForIO = false,
                    AutomaticRecoveryEnabled = false,
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
                };

                _connection = factory.CreateConnection();
                Logger.Verbose("Connection created.");
                _connection.ConnectionShutdown += OnConnectionShutdown;
                _connection.ConnectionBlocked += OnConnectionBlocked;
                _connection.ConnectionUnblocked += OnConnectionUnblocked;
            }

            if (_channel?.IsOpen != true)
            {
                Logger.Verbose("Creating a model.");
                _channel = _connection.CreateModel();
                _channel.QueueDeclare(QueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                _channel.ConfirmSelect();   // manual (N)ACK
                Logger.Verbose("Model created.");
            }
        }

        public void Dispose()
        {
            try
            {
                if (_channel?.IsClosed == false)
                {
                    _channel.Close();
                }
                _connection?.Close();
            }
            catch (Exception ex)
            {
                Logger.Error(0, "Error during RMQ connection disposal.", ex);
            }
        }

        private void OnConnectionShutdown(object connection, ShutdownEventArgs reason)
        {
            Logger.Warn(0, $"Connection was shut down: [{reason.ReplyText}]");
        }

        private void OnConnectionBlocked(object connection, ConnectionBlockedEventArgs reason)
        {
            Logger.Warn(0, $"Connection is blocked: [{reason.Reason}]");
        }

        private void OnConnectionUnblocked(object connection, EventArgs args)
        {
            Logger.Warn(0, "Connection is not blocked any more.");
        }
    }
}