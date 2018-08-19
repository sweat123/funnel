# overview

`kafka`作为一款优秀的消息队列，能够实时的传输大量的数据。并且现在越来越多的项目开始使用`kafka`。对`kafka`消费者和生产者的数据审计也越来越最重要。

作为使用`kafka`的使用方，我可能想知道，某个时间段内某个消费者从某个`topic`里消费了多少个消息，消息到达消费者应用的时候，延迟是多少；以及某个时间段内生产者发送了多少个消息到`topic`内。

基于这样的考虑，设计一款能够审计`KafkaProducer`和`KafkaConsumer`在某个时间段内消息接收、发送数量和延迟的工具。并且尽量减少对业务代码的侵入。

# 设计

## 概述
 
1. `kafka`在`0.10.x`版本之后开始引入了`interceptor`概念，`KafkaProducer`和`KafkaCosnumer`可以设置`interceptor`，让每个发送和收到的消息都经过相应的拦截器过滤。基于`interceptor`,
就可以通过对用户最少的侵入，来实现消息的审计。
2. `kafka`在`0.10.x`之后，每个消息都会带有一个时间戳。如果生产者发送消息时，没有带有时间戳，那么消息到达`kafka`服务器的时间戳会被添加到此消息上。基于消息带有的时间戳，就能够做到消息从`kafka`
服务器到消费者获取到消息这段时间的延迟。

## 详细设计

整体架构设计图

![http://or0f3pi7r.bkt.clouddn.com/18-7-26/38709579.jpg](http://or0f3pi7r.bkt.clouddn.com/18-7-26/38709579.jpg)

通过使用`Kafka`生产者和消费者的客户端，都接入`funnel`拦截器，`funnel`拦截器会对生产者生产的每条消息和消费者消费的每条消息进行处理，这个处理过程会是异步进行的，尽可能的不影响`kafka`本身的性能；
每条被`funnel interceptor`处理的消息，都会被计算分配到一个时间桶内，每个时间桶内会维护消息的个数，平均延迟等信息。这些信息最后会被发送到一个专门用于审计的`Topic`，审计系统会从
审计`Topic`读取数据，将元数据导入到`elsticsearch`，最后通过`grafana`展示元数据。

### 时间桶概念

`funnel interceptor`设计基于时间桶的概念。每一个时间戳都能被确定到一个唯一的时间桶内，时间桶的起始时间:`timestamp - timestamp%时间桶间隔`。

时间桶的概念可以简单的理解为下图:

![http://or0f3pi7r.bkt.clouddn.com/18-7-26/14165084.jpg](http://or0f3pi7r.bkt.clouddn.com/18-7-26/14165084.jpg)

1. 一个时间桶对应一个或多个`record`。
2. 时间桶会维护这段时间区间内，消息的个数等元数据。
3. 每一个`topic`的消息有相应的时间桶（可以简单的理解为当前时间戳和`Record Topic`决定了时间桶）。
4. 当前时间戳已经不在前一个时间桶时，前一个时间桶会被发送到一个审计`Topic`。

### Consumer Interceptor 设计

`KafkaConsumer`每次调用`poll`方法，从`topic`获取到一批`record`。每一个`record`都会经过`Consumer Interceptor`。对于每一个`record`，`funnel interceptor`都会经过一下几步:

1. 根据当前时间戳，计算当前的`record`应该落在哪一个时间桶内。
2. `record`携带的时间戳和当前时间戳相减，这个差值作为消息的延迟（可以理解为消息从落到`kafka`服务器，到被消费者消费这段时间的延迟）。
3. 一个时间桶对应多个`record`，时间桶内会维护消息的平均延迟、`C99`延迟、`C95`延迟、最大延迟。

整个工作过程如下图:

![http://or0f3pi7r.bkt.clouddn.com/18-7-26/17158516.jpg](http://or0f3pi7r.bkt.clouddn.com/18-7-26/17158516.jpg)

### Producer Interceptor 设计

和消费者拦截器类似，所有`KafkaProducer`发送的`record`都会经过`Producer Interceptor`，基于`Producer Interceptor`，我们也能够做到对消息的审计。但是生产者的拦截器审计方式和消费者方面会有所不同。

生产者发送的消息并不是带有时间戳的（除非发送的时候，主动带上时间戳），所以生产者这部分是不考虑延迟的。也就是说从`KafkaProducer`到`Kafka Broker`这部分的延迟时被我们忽略了。

在生产者的拦截器内，我们同样使用了时间桶。但是生产者方面的时间桶只会保存消息的个数。所以生产者部分的设计还是比较简单的。

### 审计服务

生产者和消费者拦截器审计得到的时间桶，都会被发送到一个`Audit Topic`。审计服务会不断消费`Audit Topic`，根据时间桶所带有的元数据，进行一定的转化后，导入`elsticsearch`;

### 查看

使用`grafana`将`elsticsearch`作为数据源，展示某段时间内生产者发送的`record`个数，消费者消费的`record`个数以及相应的延迟数据。

# 注意事项

[NOTICE](./NOTICE.md)

# 其它

如果有任何疑问，可以提`issues`，提出宝贵的意见！