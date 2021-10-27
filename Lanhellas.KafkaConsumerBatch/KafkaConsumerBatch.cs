using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Lanhellas.KafkaConsumerBatch
{
    public class KafkaConsumerBatch<TKey, TValue> : IKafkaConsumerBatch<TKey, TValue>
    {
        internal readonly List<ConsumeResult<TKey, TValue>> _records;
        
        internal readonly int _batchSize;
        internal readonly TimeSpan _maxWaitTime;
        internal readonly IConsumer<TKey, TValue> _consumer;
        internal readonly ILogger _logger;

        public KafkaConsumerBatch(IConsumer<TKey, TValue> consumer, int batchSize, TimeSpan maxWaitTime, ILogger logger)
        {
            _records = new List<ConsumeResult<TKey, TValue>>(batchSize);
            _batchSize = batchSize;
            _maxWaitTime = maxWaitTime;
            _consumer = consumer;
            _logger = logger;
        }

        /// <summary>
        /// Consume records in batch, return to caller when batchSize is reached or when maxWaitTime is reached
        /// </summary>
        /// <returns></returns>
        public IReadOnlyList<ConsumeResult<TKey, TValue>> ConsumeBatch()
        {
            _records.Clear();
            StartConsume();
            return _records;
        }

        /// <summary>
        /// Seek offets to minimum in batch, of all partitions that was consumed in this batch flow
        /// </summary>
        /// <returns>Offsets</returns>
        public IReadOnlyList<TopicPartitionOffset> SeekBatch()
        {
            var topicPartitionOffsets = _records
                .GroupBy(r => r.TopicPartition)
                .Select(sublist => new TopicPartitionOffset(sublist.Key, new Offset(sublist.Min(s => s.Offset.Value))))
                .ToList();

            foreach (var topicPartitionOffset in topicPartitionOffsets)
            {
                _logger.LogDebug($"Seek Partition {topicPartitionOffset.Partition.Value} to Offset {topicPartitionOffset.Offset.Value}");
                _consumer.Seek(topicPartitionOffset);
            }
            return topicPartitionOffsets;
        }

        private void StartConsume()
        {
            List<ConsumeResult<TKey, TValue>> records = _records;
            var source = new CancellationTokenSource();
            source.CancelAfter(_maxWaitTime);
            int i = 0;
            for (; i < _batchSize && !source.IsCancellationRequested; i++)
            {
                try
                {
                    records.Add(
                        _consumer.Consume(source.Token));
                    _logger.LogDebug($"Actual Records Size {records.Count}, BatchSize ${_batchSize}");
                }
                catch (OperationCanceledException)
                {
                    _logger.LogDebug($"Reached MaxWaitTime {_maxWaitTime}, Returning batch with {records.Count} results to caller");
                    break;
                }
            }
        }
    }
}