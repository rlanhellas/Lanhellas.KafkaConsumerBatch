using Confluent.Kafka;
using System.Collections.Generic;

namespace Lanhellas.KafkaConsumerBatch
{
    public interface IKafkaConsumerBatch<TKey, TValue>
    {
        IReadOnlyList<ConsumeResult<TKey, TValue>> ConsumeBatch();
        IReadOnlyList<TopicPartitionOffset> SeekBatch();
    }
}