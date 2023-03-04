Permanent Queue
=====

[![Build Status](https://github.com/qingchao-kong/permanentqueue/actions/workflows/maven.yml/badge.svg
)](https://github.com/qingchao-kong/permanentqueue)

基于 [Apache Kafka](http://kafka.apache.org/) LogManager实现的 Java 本地持久化队列.

## Background
1. 对于数据处理系统，高峰期如果数据无法进行及时处理，背压传导，会导致整个链路阻塞；此时需要通过消息队列进行削峰填谷。
   1. 引入分布式消息队列，会导致整个系统复杂度提升；
   2. 使用内存队列，存在数据丢失风险；
   3. 所以高效的本地持久化队列，符合特定场景的需求;
2. 当基础设施出现重大故障（断网、断电、地震、火灾......）的时候，系统需要可靠的降级措施；本地持久化队列可以作为数据暂存的降级方案。
## Status
1. 第一步：将Kafka LogManager的Scala代码翻译为Java代码。已完成。
   1. 基于 Apache Kafka commit [trunk/e23c59d0](https://github.com/apache/kafka/commit/e23c59d0).
2. 第二步：持久化队列实现，已完成。
3. 第三步：单测完善中。

## Usage

### Write & Read
```java
package cn.pockethub.permanentqueue.executable;

import cn.pockethub.permanentqueue.PermanentQueue;
import cn.pockethub.permanentqueue.PermanentQueueManager;
import cn.pockethub.permanentqueue.PermanentQueueManagerBuilder;
import cn.pockethub.permanentqueue.Queue;
import cn.pockethub.permanentqueue.kafka.log.*;
import cn.pockethub.permanentqueue.kafka.utils.KafkaScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class WriteReadTest {
   private static final Logger LOG = LoggerFactory.getLogger(WriteReadTest.class);

   public static void main(String[] args) throws Throwable {
      PermanentQueueManager queueManager = new PermanentQueueManagerBuilder()
              .setLogDir(new File("/tmp/permanentQueue-test"))
              .setInitialDefaultConfig(new LogConfig())
              .setCleanerConfig(new CleanerConfig())
              .setScheduler(new KafkaScheduler(2))
              .build();
      queueManager.startUp();

      PermanentQueue queue = queueManager.getOrCreatePermanentQueue("test");
      LOG.info("Initialized log at {}", queueManager.getLogDir());

      long offset = queue.write(null, "test".getBytes());
      LOG.info("Wrote message offset={}", offset);

      List<Queue.ReadEntry> read = queue.read(null);
      for (Queue.ReadEntry readEntry : read) {
         LOG.info("Read message: \"{}\" (at {})", new String(readEntry.getPayload()), readEntry.getOffset());
         //commit
         queue.markQueueOffsetCommitted(readEntry.getOffset());
      }

      queueManager.shutDown();
   }
}
```

## Todo
### 提高性能
1. 考虑将数据预读到内存队列(Disruptor)提高读取速度；
2. ......
### 完善单测
1. 目前部分单测失败；
2. 较多单测代码未翻译；

### 完善指标监控


## Reference
1. Apache Kafka commit [trunk/e23c59d0](https://github.com/apache/kafka/commit/e23c59d0).
2. https://github.com/Graylog2/graylog2-server
3. https://github.com/bernd/samsa
4. https://github.com/wangshuai1992/cockburst