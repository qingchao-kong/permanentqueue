Permanent Queue
=====

[![Build Status](https://github.com/qingchao-kong/permanentqueue/actions/workflows/maven.yml/badge.svg
)](https://github.com/qingchao-kong/permanentqueue)

基于 [Apache RocketMQ](https://rocketmq.apache.org/) DefaultMessageStore 实现的 Java 本地持久化队列.

https://zhuanlan.zhihu.com/p/619687732

## Background

1. 对于数据处理系统，高峰期如果数据无法进行及时处理，背压传导，会导致整个链路阻塞；此时需要通过消息队列进行削峰填谷。
    1. 引入分布式消息队列，会导致整个系统复杂度提升；
    2. 使用内存队列，存在数据丢失风险；
    3. 所以高效的本地持久化队列，符合特定场景的需求;
2. 当基础设施出现重大故障（断网、断电、地震、火灾......）的时候，系统需要可靠的降级措施；本地持久化队列可以作为数据暂存的降级方案。

## Feature

1. 支持多topic；
2. 支持单条消息、多条消息写入；
3. 支持批量消费；
4. 保证线程安全；

## Status

1. 第一步：持久化队列实现，已完成。
    1. 基于 Apache RocketMQ 5.1.0 [Releases/rocketmq-all-5.1.0](https://github.com/apache/rocketmq/releases/tag/rocketmq-all-5.1.0).
2. 第二步：单测完善中。

## Usage

### Write & Read

```java
package cn.pockethub.permanentqueue.executable;

import cn.pockethub.permanentqueue.PermanentQueue;
import cn.pockethub.permanentqueue.PermanentQueueConfig;
import cn.pockethub.permanentqueue.Queue;
import org.apache.rocketmq.common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.UUID;

public class WriteReadTest {
    private static final Logger LOG = LoggerFactory.getLogger(WriteReadTest.class);

    public static void main(String[] args) throws Throwable {
        UUID uuid = UUID.randomUUID();
        String baseDir = System.getProperty("java.io.tmpdir") + File.separator + "store-" + uuid;
        PermanentQueueConfig config = new PermanentQueueConfig.Builder()
                .storePath(baseDir)
                .build();
        PermanentQueue permanentQueue = new PermanentQueue(config);
        permanentQueue.startUp();

        LOG.info("Initialized PermanentQueue at {}", baseDir);

        long offset = permanentQueue.write("test", "test".getBytes());
        LOG.info("Wrote message offset={}", offset);

        List<Queue.ReadEntry> read = permanentQueue.read("test", 10);
        for (Queue.ReadEntry readEntry : read) {
            LOG.info("Read message: \"{}\" (at {})", new String(readEntry.getMessageBytes()), readEntry.getOffset());
            //commit
            permanentQueue.commit("test", readEntry.getOffset());
        }

        permanentQueue.shutDown();
        Thread.sleep(10 * 1000);

        File file = new File(baseDir);
        UtilAll.deleteFile(file);
    }
}
```

## Todo

### 完善单测

### 完善指标监控

## Reference

1. Apache RocketMQ 5.1.0 [Releases/rocketmq-all-5.1.0](https://github.com/apache/rocketmq/releases/tag/rocketmq-all-5.1.0).
2. https://github.com/Graylog2/graylog2-server
