namespace Lanhellas.KafkaConsumerBatch
{
    public interface IKafkaConsumerBatchBuilder<TKey, TValue>
    {
        IKafkaConsumerBatch<TKey, TValue> Build();
    }
}