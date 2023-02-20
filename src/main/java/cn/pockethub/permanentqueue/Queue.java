package cn.pockethub.permanentqueue;

import java.util.List;

public interface Queue {

    Entry createEntry(byte[] idBytes, byte[] messageBytes);

    long write(List<Entry> entries);

    long write(byte[] idBytes, byte[] messageBytes);

    /**
     *
     * @param maxLength The maximum number of bytes to read
     * @return
     */
    List<ReadEntry> read(Integer maxLength);

    void markQueueOffsetCommitted(long offset);

    class Entry {
        private final byte[] idBytes;
        private final byte[] messageBytes;

        public Entry(byte[] idBytes, byte[] messageBytes) {
            this.idBytes = idBytes;
            this.messageBytes = messageBytes;
        }

        public byte[] getIdBytes() {
            return idBytes;
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
