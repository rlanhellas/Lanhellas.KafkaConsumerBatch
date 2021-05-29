using Confluent.Kafka;
using Lanhellas.KafkaConsumerBatch;
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
            mockConsumer.Setup(t => t.Consume(It.IsAny<CancellationToken>()))
                .Callback(() => Thread.Sleep(maxWaitTime))
                .Throws(new OperationCanceledException());

            Assert.Empty(consumerBatch.ConsumeBatch());
        }

        [Fact]
        public void ConsumeBatch_WhenElapsedWaitTimeWithOneResult_ShouldReturnOneItem()
        {
            mockConsumer.Setup(t => t.Consume(It.IsAny<CancellationToken>()))
                .Callback(() => Thread.Sleep(maxWaitTime + TimeSpan.FromMilliseconds(50)))
                .Returns(new ConsumeResult<string, string>());

            Assert.Single(consumerBatch.ConsumeBatch());
        }

        [Fact]
        public void ConsumeBatch_WhenElapsedWaitTimeWithBatchSize_ShouldReturnCollectionWithBatchSize()
        {
            mockConsumer.Setup(t => t.Consume(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(s => s.IsCancellationRequested ? throw new OperationCanceledException() : new ConsumeResult<string, string>());

            Assert.Equal(batchSize, consumerBatch.ConsumeBatch().Count);
        }

        [Fact]
        public void SeekBatch_WhenHasOnlyOnePartitionTopic_ShouldReturnSingle()
        {
            consumerBatch._records.Add(CreateConsumeResult("", 1, 1));
            Assert.Single(consumerBatch.SeekBatch());
        }

        [Fact]
        public void SeekBatch_WhenHasOnlyOnePartitionTopic_ShouldReturnSingleWithMinOffset()
        {
            consumerBatch._records.Add(CreateConsumeResult("", 1, 1));
            consumerBatch._records.Add(CreateConsumeResult("", 1, 2));
            var seekBatchResult = consumerBatch.SeekBatch();
            Assert.Single(seekBatchResult);
            Assert.Equal(1, seekBatchResult.First().Offset);
        }

        [Theory]
        [InlineData("", 1, 1L, "a", 1, 2L)]
        [InlineData("", 1, 1L, "", 2, 2L)]
        public void SeekBatch_WhenMoreThanOnePartitionTopic_ShouldReturnSingleWithMinOffset(
            string firstTopic, int firstPartition, long firstOffset, 
            string secondTopic, int secondPartition, long secondOffset)
        {
            consumerBatch._records.Add(CreateConsumeResult(firstTopic, firstPartition, firstOffset));
            consumerBatch._records.Add(CreateConsumeResult(secondTopic, secondPartition, secondOffset));
            var seekBatchResult = consumerBatch.SeekBatch();
            Assert.Equal(2, seekBatchResult.Count);
            Assert.Equal(firstOffset, seekBatchResult[0].Offset.Value);
            Assert.Equal(secondOffset, seekBatchResult[1].Offset.Value);
        }

        private static ConsumeResult<string, string> CreateConsumeResult(string topic, int partition, long offset)
        {
            return new ConsumeResult<string, string>()
            {
                Topic = topic,
                Partition = new Partition(partition),
                Offset = new Offset(offset)
            };
        }
    }
}
