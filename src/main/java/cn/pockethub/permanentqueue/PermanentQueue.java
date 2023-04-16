package cn.pockethub.permanentqueue;

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.rocketmq.common.sysflag.MessageSysFlag.TRANSACTION_NOT_TYPE;

public class PermanentQueue extends AbstractIdleService implements Queue {
    private static final Logger LOGGER = LoggerFactory.getLogger(PermanentQueue.class);

    private static final String PermanentQueue = "PermanentQueue";

    private static final SocketAddress bornHost = new InetSocketAddress("127.0.0.1", 0);
    private static final SocketAddress storeHost = bornHost;

    private final DefaultMessageStore messageStore;

    private final ConsumerOffsetManager consumerOffsetManager;

    private final PermanentQueueConfig permanentQueueConfig;
    private final MessageStoreConfig messageStoreConfig;

    private final ScheduledExecutorService scheduledExecutorService;

    private final ConsumerLock consumerLock = new ConsumerLock();

    private volatile boolean shutdown = false;

    public PermanentQueue(PermanentQueueConfig permanentQueueConfig) throws Throwable {
        this.permanentQueueConfig = permanentQueueConfig;

        this.messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setStorePathRootDir(permanentQueueConfig.getStorePath());
        messageStoreConfig.setHaListenPort(0);
        messageStoreConfig.setMaxTransferCountOnMessageInDisk(1024);
        messageStoreConfig.setMaxTransferCountOnMessageInMemory(1024);

        this.messageStore = new DefaultMessageStore(
                messageStoreConfig,
                new BrokerStatsManager(PermanentQueue, true),
                (topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties) -> {
                },
                new BrokerConfig()
        );

        this.consumerOffsetManager = new ConsumerOffsetManager(this);

        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder()
                        .namingPattern("PermanentQueueScheduledThread")
                        .daemon(true)
                        .build()
        );
    }

    @Override
    public void startUp() throws Exception {
        LOGGER.info("---------------------------------------- PermanentQueue start up ----------------------------------------");
        consumerOffsetManager.load();
        messageStore.load();
        messageStore.start();
        initializeScheduledTasks();
        Runtime.getRuntime().addShutdownHook(new Thread(buildShutdownHook(this)));
        LOGGER.info("---------------------------------------- PermanentQueue start success ----------------------------------------");
    }

    @Override
    public void shutDown() {
        LOGGER.info("---------------------------------------- PermanentQueue shutdown ----------------------------------------");
        shutdown = true;

        if (Objects.nonNull(consumerOffsetManager)) {
            consumerOffsetManager.persist();
        }

        if (Objects.nonNull(scheduledExecutorService)) {
            scheduledExecutorService.shutdown();
        }

        if (Objects.nonNull(messageStore)) {
            messageStore.flush();
            messageStore.shutdown();
        }

        LOGGER.info("---------------------------------------- PermanentQueue shutdown success ----------------------------------------");
    }

    @Override
    public long write(String topic, byte[] messageBytes) throws QueueException {
        if (shutdown) {
            throw new QueueException("PermanentQueue has shutdown!");
        }

        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setBody(messageBytes);
        msg.setQueueId(0);
        //非事务消息
        msg.setSysFlag(TRANSACTION_NOT_TYPE);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        msg.setTags("t");

        PutMessageResult putMessageResult = messageStore.putMessage(msg);
        if (putMessageResult.isOk()) {
            return putMessageResult.getAppendMessageResult().getLogicsOffset();
        } else {
            return -1;
        }
    }

    @Override
    public long write(String topic, List<byte[]> batchMessageBytes) throws QueueException {
        if (shutdown) {
            throw new QueueException("PermanentQueue has shutdown!");
        }

        List<Message> messages = new ArrayList<>();
        for (byte[] messageBytes : batchMessageBytes) {
            Message msg = new Message();
            msg.setBody(messageBytes);
            msg.setTopic(topic);
            messages.add(msg);
        }
        byte[] batchMessageBody = MessageDecoder.encodeMessages(messages);
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(topic);
        messageExtBatch.setQueueId(0);
        //非事务消息
        messageExtBatch.setSysFlag(TRANSACTION_NOT_TYPE);
        messageExtBatch.setBody(batchMessageBody);
        messageExtBatch.setBornTimestamp(System.currentTimeMillis());
        messageExtBatch.setStoreHost(storeHost);
        messageExtBatch.setBornHost(bornHost);
        messageExtBatch.setTags("t");

        PutMessageResult putMessageResult = messageStore.putMessages(messageExtBatch);
        if (putMessageResult.isOk()) {
            return putMessageResult.getAppendMessageResult().getLogicsOffset();
        } else {
            return -1;
        }
    }

    @Override
    public List<Queue.ReadEntry> read(String topic, int maxMsgNums) {
        if (shutdown) {
            return new ArrayList<>(0);
        }

        List<Queue.ReadEntry> readEntries = new ArrayList<>();

        GetMessageResult getMessageResult = null;
        consumerLock.lock(topic);
        try {
            long nextReadOffset = consumerOffsetManager.getNextReadOffset(topic);
            getMessageResult = messageStore.getMessage(topic, topic, 0, nextReadOffset, maxMsgNums, null);
            long offset = -1;
            for (SelectMappedBufferResult selectMappedBufferResult : getMessageResult.getMessageMapedList()) {
                MessageExt messageExt = MessageDecoder.decode(selectMappedBufferResult.getByteBuffer());
                readEntries.add(new Queue.ReadEntry(messageExt.getBody(), messageExt.getQueueOffset()));
                offset = messageExt.getQueueOffset();
            }
            if (offset >= 0) {
                consumerOffsetManager.setNextReadOffset(topic, offset + 1);
            }
        } catch (Throwable throwable) {
            LOGGER.error("Decode message error.", throwable);
        } finally {
            consumerLock.unlock(topic);
            if (null != getMessageResult) {
                getMessageResult.release();
            }
        }

        return readEntries;
    }

    @Override
    public void commit(String topic, long offset) throws QueueException {
        if (shutdown) {
            throw new QueueException("PermanentQueue has shutdown!");
        }

        consumerOffsetManager.commitOffset(topic, offset);
    }

    private void initializeScheduledTasks() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    PermanentQueue.this.consumerOffsetManager.persist();
                } catch (Throwable e) {
                    LOGGER.error("permanentQueue: failed to persist config file of consumerOffset", e);
                }
            }
        }, 1000, this.permanentQueueConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);
    }

    private static Runnable buildShutdownHook(PermanentQueue permanentQueue) {
        return new Runnable() {
            private volatile boolean hasShutdown = false;
            private final AtomicInteger shutdownTimes = new AtomicInteger(0);

            @Override
            public void run() {
                synchronized (this) {
                    LOGGER.info("PermanentQueue shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        permanentQueue.shutDown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        LOGGER.info("PermanentQueue shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        };
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    private static class ConsumerLock {
        private final ConcurrentMap<String, ReentrantLock> lockMap = new ConcurrentHashMap<>();

        public void lock(String topic) {
            lockMap.computeIfAbsent(topic, k -> new ReentrantLock())
                    .lock();
        }

        public void unlock(String topic) {
            lockMap.get(topic)
                    .unlock();
        }
    }
}
