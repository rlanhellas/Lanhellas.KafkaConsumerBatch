# Introduction
This library born from a needed to consume messages in batch style, using the famous Confluent.Kafka library (https://www.nuget.org/packages/Confluent.Kafka). It works as a Wrapper providing the capability to return a List of results instead of a single result *(see a real example in the 'How to use ?' section).*


## How to use?

You first need to create a consumer from *Confluent.Kafka* and **subscribe** in a topic, see:

```c#
var config = new ConsumerConfig  
{  
  BootstrapServers = "localhost:9092",  
  GroupId = "mygroup"
};  
var consumer = new ConsumerBuilder<string,string>(config).Build();  
consumer.Subscribe(topic);
```

Until here, nothing new, right? :D 

Now you will need to create the **Batch Wrapper,** using the Builder Pattern, see how:

```c#
var consumerBatch = KafkaConsumerBatchBuilder<string, string>.Config()  
 .WithConsumer(consumer)  
 .WithLogger(_logger)  
 .WithBatchSize(50)  
 .WithMaxWaitTime(TimeSpan.FromSeconds(10))  
 .Build();
```

Take note of some important properties:

 1. `WithConsumer(IConsumer<TKey, TValue> consumer)`: You must pass the consumer object that you created before with `new ConsumerBuilder<,>()`
 2. `WithLogger(ILogger logger)`: You must pass an instance of ILogger, in this way you can trace any problems.
 3. `WithBatchSize(int batchSize)`: How many records do you want to ? Inform here the quantity.
 4. `WithMaxWaitTime(TimeSpan maxWaitTime)`: Inform here how many times do you want to wait before returning the consumer. If *batchSize* is not reached before *maxWaitTime* the batch will return immediately.

And finally, you can consume in batch:

```c#
var results = consumerBatch.ConsumeBatch();
```

Now you can seek (redelivery all messages) in batch too:

```c#
consumerBatch.SeekBatch();
```

And if you need to commit, just do it, the same way as you usually do (**You don't need to use consumerBatch here**):

```c#
consumer.Commit();
```


## You can contribute

Feel free to contribute with your suggestions and improvements, just make a Pull Request and be happy. 
