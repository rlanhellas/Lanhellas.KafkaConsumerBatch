using System;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Lanhellas.KafkaConsumerBatch
{
    public class KafkaConsumerBatchBuilder<TKey,TValue>
    {
        private IConsumer<TKey, TValue> _consumer;
        private int _batchSize;
        private TimeSpan _maxWaitTime;
        private ILogger _logger;

        public static KafkaConsumerBatchBuilder<TKey,TValue> Config()
        {
            return new KafkaConsumerBatchBuilder<TKey, TValue>();
        }
        
        public KafkaConsumerBatchBuilder<TKey,TValue> WithConsumer(IConsumer<TKey, TValue> consumer)
        {
            _consumer = consumer;
            return this;
        }
        
        public KafkaConsumerBatchBuilder<TKey,TValue> WithBatchSize(int batchSize)
        {
            _batchSize = batchSize;
            return this;
        }
        
        public KafkaConsumerBatchBuilder<TKey,TValue> WithMaxWaitTime(TimeSpan maxWaitTime)
        {
            _maxWaitTime = maxWaitTime;
            return this;
        }
        
        public KafkaConsumerBatchBuilder<TKey,TValue> WithLogger(ILogger logger)
        {
            _logger = logger;
            return this;
        } 

        public KafkaConsumerBatch<TKey, TValue> Build()
        {
            return new KafkaConsumerBatch<TKey, TValue>(_consumer, _batchSize, _maxWaitTime, _logger);
        }
    }
}