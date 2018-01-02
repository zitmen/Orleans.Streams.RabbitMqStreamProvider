using System;

namespace Orleans.Streams.RabbitMq
{
    [Serializable]
    public class RabbitMqException : Exception
    {
        private readonly string _rmqClientExceptionStackTrace;

        public override string StackTrace => _rmqClientExceptionStackTrace;

        public RabbitMqException(string message, Exception innerException)
            : base($"{message} [{innerException.Message}]")
        {
            _rmqClientExceptionStackTrace = innerException.StackTrace;
        }

        public override string ToString()
            => Message + Environment.NewLine + StackTrace;
    }
}