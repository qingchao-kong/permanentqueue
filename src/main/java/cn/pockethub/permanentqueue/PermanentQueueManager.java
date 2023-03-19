package cn.pockethub.permanentqueue;

import cn.pockethub.permanentqueue.rocketmq.broker.ConsumerOffsetManager;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.Getter;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class PermanentQueueManager extends AbstractIdleService {
    private static final Logger LOG = LoggerFactory.getLogger(PermanentQueueManager.class);

    private static final SocketAddress bornHost = new InetSocketAddress("127.0.0.1", 8123);
    private static final SocketAddress storeHost = bornHost;

    private final DefaultMessageStore messageStore;
    private final ConsumerOffsetManager consumerOffsetManager;
    private final QueueConfig queueConfig;
    private final MessageStoreConfig messageStoreConfig;
    private final ScheduledExecutorService scheduledExecutorService;

    private volatile boolean shutdown = false;

    public PermanentQueueManager() throws Throwable {
        String baseDir = System.getProperty("java.io.tmpdir") + File.separator + "store-kqc";

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
//        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 8);
//        messageStoreConfig.setMappedFileSizeConsumeQueue(100 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
//        messageStoreConfig.setMapperFileSizeBatchConsumeQueue(20 * BatchConsumeQueue.CQ_STORE_UNIT_SIZE);
//        messageStoreConfig.setMappedFileSizeConsumeQueueExt(1024);
//        messageStoreConfig.setMaxIndexNum(100 * 10);
//        messageStoreConfig.setEnableConsumeQueueExt(true);
        messageStoreConfig.setStorePathRootDir(baseDir);
//        messageStoreConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        messageStoreConfig.setHaListenPort(0);
//        messageStoreConfig.setMaxTransferBytesOnMessageInDisk(1024 * 1024);
//        messageStoreConfig.setMaxTransferBytesOnMessageInMemory(1024 * 1024);
//        messageStoreConfig.setMaxTransferCountOnMessageInDisk(1024);
//        messageStoreConfig.setMaxTransferCountOnMessageInMemory(1024);

//        messageStoreConfig.setFlushIntervalCommitLog(1);
//        messageStoreConfig.setFlushCommitLogThoroughInterval(2);

        this.messageStore = new DefaultMessageStore(
                messageStoreConfig,
                new BrokerStatsManager("simpleTest", true),
                (topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties) -> {
                },
                new BrokerConfig()
        );

        this.messageStoreConfig = messageStore.getMessageStoreConfig();
        this.consumerOffsetManager = new ConsumerOffsetManager(this);
        this.queueConfig = new QueueConfig();

        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder()
                        .namingPattern("PermanentQueueManagerScheduledThread")
                        .daemon(true)
                        .build()
        );
    }

    @Override
    public void startUp() throws Exception {
        LOG.info("---------------------------------------- PermanentQueueManager start up ----------------------------------------");
        consumerOffsetManager.load();
        messageStore.load();
        messageStore.start();
        initializeScheduledTasks();
        Runtime.getRuntime().addShutdownHook(new Thread(buildShutdownHook(this)));
        LOG.info("---------------------------------------- PermanentQueueManager start success ----------------------------------------");
    }

    @Override
    public void shutDown() {
        LOG.info("---------------------------------------- PermanentQueueManager shutdown ----------------------------------------");
        shutdown = true;
        if (Objects.nonNull(messageStore)) {
            messageStore.flush();
            messageStore.shutdown();
        }
        if (Objects.nonNull(consumerOffsetManager)) {
            consumerOffsetManager.persist();
        }

        if (Objects.nonNull(scheduledExecutorService)) {
            scheduledExecutorService.shutdown();
        }
        LOG.info("---------------------------------------- PermanentQueueManager shutdown success ----------------------------------------");
    }

    public boolean write(String topic, byte[] messageBytes) throws PermanentQueueException {
        if (shutdown) {
            throw new PermanentQueueException("PermanentQueueManager has shutdown!");
        }

        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setBody(messageBytes);
        msg.setQueueId(0);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(storeHost);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_NUM, String.valueOf(-1));
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_INNER_NUM);

        PutMessageResult putMessageResult = messageStore.putMessage(msg);
        return putMessageResult.isOk();
    }

    public boolean write(String topic, List<byte[]> batchMessageBytes) throws PermanentQueueException {
        if (shutdown) {
            throw new PermanentQueueException("PermanentQueueManager has shutdown!");
        }

        List<Message> messages = new ArrayList<>();
        for (byte[] messageBytes : batchMessageBytes) {
            Message msg = new Message();
            msg.setBody(messageBytes);
            msg.setTopic(topic);
//            msg.setTags("TAG1");
//            msg.setKeys(String.valueOf(System.currentTimeMillis()));
            messages.add(msg);
        }
        byte[] batchMessageBody = MessageDecoder.encodeMessages(messages);
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(topic);
        messageExtBatch.setQueueId(0);
        messageExtBatch.setBody(batchMessageBody);
//        messageExtBatch.putUserProperty(batchPropK, batchPropV);
        messageExtBatch.setBornTimestamp(System.currentTimeMillis());
        messageExtBatch.setStoreHost(new InetSocketAddress("127.0.0.1", 125));
        messageExtBatch.setBornHost(new InetSocketAddress("127.0.0.1", 126));

        PutMessageResult putMessageResult = messageStore.putMessages(messageExtBatch);
        return putMessageResult.isOk();
    }

    public List<ReadEntry> read(String topic, int maxMsgNums) throws PermanentQueueException {
        if (shutdown) {
            throw new PermanentQueueException("PermanentQueueManager has shutdown!");
        }

        long offset = consumerOffsetManager.queryOffset(topic, topic, 0);
        long nextReadOffset = offset < 0 ? 0 : offset + 1;
        GetMessageResult getMessageResult = messageStore.getMessage(topic, topic, 0, nextReadOffset, maxMsgNums, null);
        List<ReadEntry> readEntries = new ArrayList<>(getMessageResult.getMessageCount());

        try {
            for (SelectMappedBufferResult selectMappedBufferResult : getMessageResult.getMessageMapedList()) {
                MessageExt messageExt = MessageDecoder.decode(selectMappedBufferResult.getByteBuffer());
                readEntries.add(new ReadEntry(messageExt.getBody(), messageExt.getQueueOffset()));
            }
        } catch (Throwable throwable) {
            LOG.error("Decode message error.", throwable);
        } finally {
            getMessageResult.release();
        }

        return readEntries;
    }

    public void commit(String topic, long offset) throws PermanentQueueException {
        if (shutdown) {
            throw new PermanentQueueException("PermanentQueueManager has shutdown!");
        }

        consumerOffsetManager.commitOffset(null, topic, topic, 0, offset);
    }

    private void initializeScheduledTasks() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    PermanentQueueManager.this.consumerOffsetManager.persist();
                } catch (Throwable e) {
                    LOG.error("QueueManager: failed to persist config file of consumerOffset", e);
                }
            }
        }, 1000 * 10, this.queueConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);
    }

    private static Runnable buildShutdownHook(PermanentQueueManager permanentQueueManager) {
        return new Runnable() {
            private volatile boolean hasShutdown = false;
            private final AtomicInteger shutdownTimes = new AtomicInteger(0);

            @Override
            public void run() {
                synchronized (this) {
                    LOG.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        permanentQueueManager.shutDown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        LOG.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        };
    }

    public static class ReadEntry {

        private final byte[] messageBytes;

        private final long offset;

        public ReadEntry(byte[] messageBytes, long offset) {
            this.messageBytes = messageBytes;
            this.offset = offset;
        }

        public long getOffset() {
            return offset;
        }

        public byte[] getMessageBytes() {
            return messageBytes;
        }
    }
}
