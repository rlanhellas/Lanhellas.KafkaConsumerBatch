using Confluent.Kafka;
using Lanhellas.KafkaConsumerBatch;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System;
using System.Linq;
using System.Threading;
using Xunit;

namespace Lanhellas.KafkaConsumerBatchUnitTests
{
    public class KafkaConsumerBatchUnitTests
    {
        private readonly Mock<IConsumer<string, string>> mockConsumer;
        private readonly KafkaConsumerBatch<string, string> consumerBatch;
        private readonly int batchSize;
        private readonly TimeSpan maxWaitTime;

        public KafkaConsumerBatchUnitTests()
        {
            batchSize = 10;
            maxWaitTime = TimeSpan.FromMilliseconds(100);
            mockConsumer = new Mock<IConsumer<string, string>>();
            consumerBatch = new KafkaConsumerBatch<string, string>(mockConsumer.Object,
                batchSize, maxWaitTime, NullLogger.Instance);
        }

        [Fact]
        public void ConsumeBatch_WhenElapsedWaitTimeWithNoResults_ShouldReturnEmptyCollection()
        {
            mockConsumer.Setup(t => t.Consume(It.IsAny<TimeSpan>()))
                .Callback(() => Thread.Sleep(maxWaitTime))
                .Returns((ConsumeResult<string, string>)null);

            Assert.Empty(consumerBatch.ConsumeBatch());
        }

        [Fact]
        public void ConsumeBatch_WhenElapsedWaitTimeWithOneResult_ShouldReturnOneItem()
        {
            mockConsumer.Setup(t => t.Consume(It.IsAny<TimeSpan>()))
                .Callback(() => Thread.Sleep(maxWaitTime))
                .Returns(new ConsumeResult<string, string>());

            Assert.Single(consumerBatch.ConsumeBatch());
        }

        [Fact]
        public void ConsumeBatch_WhenElapsedWaitTimeWithBatchSize_ShouldReturnCollectionWithBatchSize()
        {
            mockConsumer.Setup(t => t.Consume(It.IsAny<TimeSpan>()))
                .Returns(new ConsumeResult<string, string>());

            Assert.Equal(batchSize, consumerBatch.ConsumeBatch().Count());
        }


    }
}
