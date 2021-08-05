using Confluent.Kafka;
using Lanhellas.KafkaConsumerBatch;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Lanhellas.KafkaConsumerBatchUnitTests
{
    public class KafkaConsumerBatchBuilderUnitTests
    {
        [Fact]
        public void Build_WithNoConfig_ShouldHaveDefaultValues()
        {
            var consumer = KafkaConsumerBatchBuilder<string, string>.Config()
                .Build() as KafkaConsumerBatch<string, string>;

            Assert.Null(consumer._consumer);
            Assert.Null(consumer._logger);
            Assert.Equal(0, consumer._batchSize);
            Assert.Equal(TimeSpan.Zero, consumer._maxWaitTime);
        }

        [Fact]
        public void Build_WithBatchSize_ShouldConfigureWithDefinedValue()
        {
            int expectedBatchSize = 133;
            var consumer = KafkaConsumerBatchBuilder<string, string>.Config()
                .WithBatchSize(expectedBatchSize)
                .Build() as KafkaConsumerBatch<string, string>;

            Assert.Equal(expectedBatchSize, consumer._batchSize);
        }

        [Fact]
        public void Build_WithMaxWaitTime_ShouldConfigureWithDefinedValue()
        {
            TimeSpan expectedMaxWaitTime = TimeSpan.FromMilliseconds(133);
            var consumer = KafkaConsumerBatchBuilder<string, string>.Config()
                .WithMaxWaitTime(expectedMaxWaitTime)
                .Build() as KafkaConsumerBatch<string, string>;

            Assert.Equal(expectedMaxWaitTime, consumer._maxWaitTime);
        }

        [Fact]
        public void Build_WithConsumer_ShouldConfigureWithDefinedValue()
        {
            IConsumer<string, string> expectedConsumer = new Mock<IConsumer<string, string>>().Object;
            var consumer = KafkaConsumerBatchBuilder<string, string>.Config()
                .WithConsumer(expectedConsumer)
                .Build() as KafkaConsumerBatch<string, string>;

            Assert.Same(expectedConsumer, consumer._consumer);
        }

        [Fact]
        public void Build_WithLogger_ShouldConfigureWithDefinedValue()
        {
            ILogger expectedLogger = new Mock<ILogger>().Object;
            var consumer = KafkaConsumerBatchBuilder<string, string>.Config()
                .WithLogger(expectedLogger)
                .Build() as KafkaConsumerBatch<string, string>;

            Assert.Same(expectedLogger, consumer._logger);
        }

        [Fact]
        public void Build_WithMultipleSets_ShouldConsiderLastValue()
        {
            int expectedBatchSize = 133;
            var consumer = KafkaConsumerBatchBuilder<string, string>.Config()
                .WithBatchSize(199)
                .WithBatchSize(expectedBatchSize)
                .Build() as KafkaConsumerBatch<string, string>;

            Assert.Equal(expectedBatchSize, consumer._batchSize);
        }
    }
}
