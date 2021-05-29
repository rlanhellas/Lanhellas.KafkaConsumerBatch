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
        private readonly List<ConsumeResult<TKey, TValue>> _records;
        private readonly int _batchSize;
        private readonly TimeSpan _maxWaitTime;
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly ILogger _logger;

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
        /// <param name="consumer"></param>
        public void SeekBatch()
        {
            var sublists = _records.GroupBy(r => r.Partition).ToList();

            foreach (var sublist in sublists)
            {
                var minOffset = sublist.Min(s => s.Offset.Value);
                var partition = sublist.Key.Value;
                _logger.LogDebug("Seek Partition {} to Offset {}", partition, minOffset);
                _consumer.Seek(new TopicPartitionOffset(sublist.First().Topic, new Partition(partition), new Offset(minOffset)));
            }
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
                    _logger.LogDebug("Actual Records Size {}, BatchSize {}", records.Count, _batchSize);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogDebug("Reached MaxWaitTime {}, Returning batch with {} results to caller", _maxWaitTime, records.Count);
                    break;
                }
            }
        }
    }
}