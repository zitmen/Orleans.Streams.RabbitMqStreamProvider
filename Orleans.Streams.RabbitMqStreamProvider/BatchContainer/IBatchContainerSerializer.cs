namespace Orleans.Streams.BatchContainer
{
    public interface IBatchContainerSerializer
    {
        byte[] Serialize(RabbitMqBatchContainer container);
        RabbitMqBatchContainer Deserialize(byte[] data);
    }
}