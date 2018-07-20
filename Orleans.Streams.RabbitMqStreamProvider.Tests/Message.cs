using ProtoBuf;
using System;

namespace RabbitMqStreamTests
{
    [Serializable]
    [ProtoContract]
    public class Message
    {
        [ProtoMember(1)]
        public readonly int Id;

        [ProtoMember(2)]
        public readonly double WorkTimeOutMillis;

        [ProtoMember(3)]
        public readonly bool Delivered;

        [ProtoMember(4)]
        public readonly string ProcessedBy;

        public Message()
        {
        }

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