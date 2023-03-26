package cn.pockethub.permanentqueue;

import java.util.List;

public interface Queue {

    long write(String topic, byte[] messageBytes) throws QueueException;

    long write(String topic, List<byte[]> batchMessageBytes) throws QueueException;

    /**
     * Query at most <code>maxMsgNums</code> messages belonging to <code>topic</code> starting from given <code>offset</code>.
     *
     * @param topic
     * @param maxMsgNums Maximum count of messages to query.
     * @return
     */
    List<ReadEntry> read(String topic, int maxMsgNums) throws QueueException;

    void commit(String topic, long offset) throws QueueException;

    class ReadEntry {

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
