using System;

namespace RabbitMqStreamTests
{
    [Serializable]
    public class Message
    {
        public readonly int Id;
        public readonly double WorkTimeOutMillis;
        public readonly bool Delivered;
        public readonly string ProcessedBy;

        public Message(int id, double timeoutMillis)
        {
            Id = id;
            WorkTimeOutMillis = timeoutMillis;
            Delivered = false;
            ProcessedBy = null;
        }

        private Message(int id, double timeout, string grainIdentifier)
        {
            Id = id;
            WorkTimeOutMillis = timeout;
            ProcessedBy = grainIdentifier;
            Delivered = true;
        }

        public override bool Equals(object obj)
        {
            var msg = obj as Message;
            return msg != null
                && Id == msg.Id
                && Delivered == msg.Delivered
                && ProcessedBy == msg.ProcessedBy;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Id;
                hashCode = (hashCode * 397) ^ WorkTimeOutMillis.GetHashCode();
                hashCode = (hashCode * 397) ^ Delivered.GetHashCode();
                hashCode = (hashCode * 397) ^ (ProcessedBy?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public Message CreateDelivered(string processor)
            => new Message(Id, WorkTimeOutMillis, processor);
    }
}