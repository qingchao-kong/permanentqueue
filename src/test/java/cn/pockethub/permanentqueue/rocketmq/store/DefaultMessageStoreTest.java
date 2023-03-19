package cn.pockethub.permanentqueue.rocketmq.store;

import com.google.common.base.Strings;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultMessageStoreTest {
    private final String storeMessage = "Once, there was a chance for me!";
    private final String messageTopic = "FooBar";
    private int queueTotal = 100;
    private AtomicInteger queueId = new AtomicInteger(0);
    private SocketAddress bornHost;
    private SocketAddress storeHost;
    private byte[] messageBody;
    private MessageStore messageStore;

    @BeforeEach
    public void init() throws Exception {
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);

        messageStore = buildMessageStore();
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
    }

    @AfterEach
    public void destroy() {
        messageStore.shutdown();
        messageStore.destroy();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    @Test
    public void testWriteAndRead() {
        long ipv4HostMsgs = 10;
        long ipv6HostMsgs = 10;
        long totalMsgs = ipv4HostMsgs + ipv6HostMsgs;
        queueTotal = 1;
        messageBody = storeMessage.getBytes();
        for (long i = 0; i < ipv4HostMsgs; i++) {
            messageStore.putMessage(buildMessage());
        }

        for (long i = 0; i < ipv6HostMsgs; i++) {
            messageStore.putMessage(buildIPv6HostMessage());
        }

//        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = messageStore.getMessage("GROUP_A", "FooBar", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            result.release();
        }
        verifyThatMasterIsFunctional(totalMsgs, messageStore);
    }

    private MessageStore buildMessageStore() throws Exception {
        return buildMessageStore(null);
    }

    private MessageStore buildMessageStore(String storePathRootDir) throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        messageStoreConfig.setHaListenPort(0);
        if (Strings.isNullOrEmpty(storePathRootDir)) {
            UUID uuid = UUID.randomUUID();
            storePathRootDir = System.getProperty("java.io.tmpdir") + File.separator + "store-" + uuid.toString();
        }
        messageStoreConfig.setStorePathRootDir(storePathRootDir);
        return new DefaultMessageStore(messageStoreConfig,
                new BrokerStatsManager("simpleTest", true),
                new MyMessageArrivingListener(),
                new BrokerConfig());
    }

    private MessageExtBrokerInner buildMessage() {
        return buildMessage(messageBody, messageTopic);
    }

    private MessageExtBrokerInner buildMessage(byte[] messageBody, String topic) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(messageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(queueId.getAndIncrement()) % queueTotal);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }

    private MessageExtBrokerInner buildIPv6HostMessage() {
        return buildIPv6HostMessage(messageBody, "FooBar");
    }

    private MessageExtBrokerInner buildIPv6HostMessage(byte[] messageBody, String topic) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(messageBody);
        msg.setMsgId("24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0");
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(queueId.getAndIncrement()) % queueTotal);
        msg.setSysFlag(0);
        msg.setBornHostV6Flag();
        msg.setStoreHostAddressV6Flag();
        msg.setBornTimestamp(System.currentTimeMillis());
        try {
            msg.setBornHost(new InetSocketAddress(InetAddress.getByName("1050:0000:0000:0000:0005:0600:300c:326b"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        try {
            msg.setStoreHost(new InetSocketAddress(InetAddress.getByName("::1"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
        return msg;
    }

    private void verifyThatMasterIsFunctional(long totalMsgs, MessageStore master) {
        for (long i = 0; i < totalMsgs; i++) {
            master.putMessage(buildMessage());
        }

//        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);

        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = master.getMessage("GROUP_A", "FooBar", 0, i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            result.release();

        }
    }

    private class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime,
                             byte[] filterBitMap, Map<String, String> properties) {
        }
    }
}
