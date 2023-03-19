package cn.pockethub.permanentqueue;

import lombok.Getter;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class QueueManager {
    private static final Logger LOG = LoggerFactory.getLogger(QueueManager.class);

    private static final int queueId = 0;

    private final QueueConfig queueConfig;
    private MessageStore messageStore;
    private final MessageStoreConfig messageStoreConfig;
//    private final ConsumerOffsetManager consumerOffsetManager;
    private ScheduledExecutorService scheduledExecutorService;

    private volatile boolean shutdown = false;

    /**
     * 初始化配置
     *
     * @throws IOException
     */
    public QueueManager() {
        queueConfig = new QueueConfig();

        messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        messageStoreConfig.setHaListenPort(0);
        UUID uuid = UUID.randomUUID();
        String storePathRootDir = System.getProperty("java.io.tmpdir") + File.separator + "store-kqc";
        messageStoreConfig.setStorePathRootDir(storePathRootDir);

        //init consumer offset manager
//        consumerOffsetManager = new ConsumerOffsetManager(this);
    }

//    public static QueueManager createQueueManager() {
//        try {
//            QueueManager queueManager = new QueueManager();
//            boolean initResult = queueManager.initialize();
//            if (!initResult) {
//                queueManager.shutdown();
//                return null;
//            }
//            Runtime.getRuntime().addShutdownHook(new Thread(buildShutdownHook(queueManager)));
//            return queueManager;
//        } catch (Throwable throwable) {
//            LOG.error("QueueManager: failed to create QueueManager", throwable);
//            return null;
//        }
//    }

    public void startUp() throws Exception {
        if (Objects.nonNull(messageStore)) {
            messageStore.start();
        }
    }

    public void shutdown() {
        if (Objects.nonNull(messageStore)) {
            messageStore.shutdown();
        }
    }

    public boolean write(String topic, byte[] messageBytes) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setBody(messageBytes);
        msg.setQueueId(queueId);
        msg.setBornHostV6Flag();
        msg.setStoreHostAddressV6Flag();
        msg.setBornHost(new InetSocketAddress(0));
        msg.setStoreHost(new InetSocketAddress(0));
        MessageAccessor.setProperties(msg,new HashMap<>(0));

        PutMessageResult putMessageResult = messageStore.putMessage(msg);
        return putMessageResult.isOk();
    }

//    public GetMessageResult read(String topic, Integer maxMsgNums) {
//        long pullOffset = consumerOffsetManager.queryPullOffset(topic, topic, queueId);
//        return messageStore.getMessage(topic, topic, queueId, pullOffset, maxMsgNums, null);
//    }
//
//    public void markQueueOffsetCommitted(String topic, long offset) {
//        consumerOffsetManager.commitPullOffset(null, topic, topic, queueId, offset);
//    }

    /**
     * 初始化资源
     *
     * @return
     */
//    private boolean initialize() {
//        //load consumer offset manager
//        boolean result = consumerOffsetManager.load();
//
//        if (result) {
//            try {
//                messageStore = new DefaultMessageStore(
//                        messageStoreConfig,
//                        new BrokerStatsManager("simpleTest", true),
//                        new MyMessageArrivingListener(),
//                        new BrokerConfig()
//                );
//            } catch (IOException e) {
//                result = false;
//                LOG.error("QueueManager#initialize: unexpected error occurs", e);
//            }
//        }
//        if (Objects.nonNull(messageStore)) {
//            registerMessageStoreHook();
//            result &= messageStore.load();
//        }
//
//        if (result) {
//            initializeResources();
//            initializeScheduledTasks();
//        }
//
//        return result;
//    }

    /**
     * Initialize resources including remoting server and thread executors.
     */
    private void initializeResources() {
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder()
                        .namingPattern("QueueControllerScheduledThread")
                        .daemon(true)
                        .build()
        );
    }

//    private void initializeScheduledTasks() {
//        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    QueueManager.this.consumerOffsetManager.persist();
//                } catch (Throwable e) {
//                    LOG.error("QueueManager: failed to persist config file of consumerOffset", e);
//                }
//            }
//        }, 1000 * 10, this.queueConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);
//    }

    private void registerMessageStoreHook() {
//        List<PutMessageHook> putMessageHookList = messageStore.getPutMessageHookList();
//
//        putMessageHookList.add(new PutMessageHook() {
//            @Override
//            public String hookName() {
//                return "checkBeforePutMessage";
//            }
//
//            @Override
//            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
//                return HookUtils.checkBeforePutMessage(QueueManager.this, msg);
//            }
//        });
//
//        putMessageHookList.add(new PutMessageHook() {
//            @Override
//            public String hookName() {
//                return "innerBatchChecker";
//            }
//
//            @Override
//            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
//                if (msg instanceof MessageExtBrokerInner) {
//                    return HookUtils.checkInnerBatch(QueueManager.this, msg);
//                }
//                return null;
//            }
//        });
//
//        putMessageHookList.add(new PutMessageHook() {
//            @Override
//            public String hookName() {
//                return "handleScheduleMessage";
//            }
//
//            @Override
//            public PutMessageResult executeBeforePutMessage(MessageExt msg) {
//                if (msg instanceof MessageExtBrokerInner) {
//                    return HookUtils.handleScheduleMessage(QueueManager.this, (MessageExtBrokerInner) msg);
//                }
//                return null;
//            }
//        });
//
//        SendMessageBackHook sendMessageBackHook = new SendMessageBackHook() {
//            @Override
//            public boolean executeSendMessageBack(List<MessageExt> msgList, String brokerName, String brokerAddr) {
//                return HookUtils.sendMessageBack(QueueManager.this, msgList, brokerName, brokerAddr);
//            }
//        };
//
//        if (messageStore != null) {
//            messageStore.setSendMessageBackHook(sendMessageBackHook);
//        }
    }

    private static Runnable buildShutdownHook(QueueManager queueManager) {
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
                        queueManager.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        LOG.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        };
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {
        }
    }

    class Entry {
        private final byte[] messageBytes;

        public Entry(byte[] messageBytes) {
            this.messageBytes = messageBytes;
        }

        public byte[] getMessageBytes() {
            return messageBytes;
        }
    }

    class ReadEntry {

        private final byte[] payload;
        private final long offset;

        public ReadEntry(byte[] payload, long offset) {
            this.payload = payload;
            this.offset = offset;
        }

        public long getOffset() {
            return offset;
        }

        public byte[] getPayload() {
            return payload;
        }
    }
}
