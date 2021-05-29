using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Lanhellas.KafkaConsumerBatch
{
    public class KafkaConsumerBatch<TKey,TValue>
    {

        private readonly ICollection<ConsumeResult<TKey, TValue>> _records;
        private readonly int _batchSize;
        private readonly TimeSpan _maxWaitTime;
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly ILogger _logger;

        public KafkaConsumerBatch(IConsumer<TKey,TValue> consumer, int batchSize, TimeSpan maxWaitTime, ILogger logger)
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
        public IEnumerable<ConsumeResult<TKey,TValue>> ConsumeBatch()
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
                _consumer.Seek(new TopicPartitionOffset(sublist.First().Topic, new Partition(partition), new Offset(minOffset) ));
            }
        }

        private void StartConsume()
        {
            while (true)
            {
                _logger.LogDebug("Actual Records Size {}, BatchSize {}", _records.Count, _batchSize);
                if (_records.Count >= _batchSize)
                {
                    break;
                }

                var result = _consumer.Consume(_maxWaitTime);
                if (result != null)
                {
                    _records.Add(result);
                }
                else
                {
                    _logger.LogDebug("Reached MaxWaitTime {}, Returning batch with {} results to caller", _maxWaitTime, _records.Count);
                    break;
                }
            }
        }
    }
}