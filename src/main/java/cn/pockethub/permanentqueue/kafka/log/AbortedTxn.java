package cn.pockethub.permanentqueue.kafka.log;

import lombok.Getter;
import org.apache.kafka.common.message.FetchResponseData;

import java.nio.ByteBuffer;

@Getter
public class AbortedTxn {

    public static final int VersionOffset = 0;
    public static final int VersionSize = 2;
    public static final int ProducerIdOffset = VersionOffset + VersionSize;
    public static final int ProducerIdSize = 8;
    public static final int FirstOffsetOffset = ProducerIdOffset + ProducerIdSize;
    public static final int FirstOffsetSize = 8;
    public static final int LastOffsetOffset = FirstOffsetOffset + FirstOffsetSize;
    public static final int LastOffsetSize = 8;
    public static final int LastStableOffsetOffset = LastOffsetOffset + LastOffsetSize;
    public static final int LastStableOffsetSize = 8;
    public static final int TotalSize = LastStableOffsetOffset + LastStableOffsetSize;

    public static final Short CurrentVersion = 0;

    private ByteBuffer buffer;

    public AbortedTxn(CompletedTxn completedTxn,
                      Long lastStableOffset) {
        this(completedTxn.getProducerId(),
                completedTxn.getFirstOffset(),
                completedTxn.getLastOffset(),
                lastStableOffset);
    }

    public AbortedTxn(Long producerId,
                      Long firstOffset,
                      Long lastOffset,
                      Long lastStableOffset) {
        this(ByteBuffer.allocate(AbortedTxn.TotalSize));
        buffer.putShort(CurrentVersion);
        buffer.putLong(producerId);
        buffer.putLong(firstOffset);
        buffer.putLong(lastOffset);
        buffer.putLong(lastStableOffset);
        buffer.flip();
    }

    public AbortedTxn(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public Short version() {
        return (short) buffer.get(VersionOffset);
    }

    public Long producerId() {
        return buffer.getLong(ProducerIdOffset);
    }

    public Long firstOffset() {
        return buffer.getLong(FirstOffsetOffset);
    }

    public Long lastOffset() {
        return buffer.getLong(LastOffsetOffset);
    }

    public Long lastStableOffset() {
        return buffer.getLong(LastStableOffsetOffset);
    }

    public FetchResponseData.AbortedTransaction asAbortedTransaction() {
        return new FetchResponseData.AbortedTransaction()
                .setProducerId(producerId())
                .setFirstOffset(firstOffset());
    }

    @Override
    public String toString() {
        return String.format("AbortedTxn(version=%s, producerId=%s, firstOffset=%s, " +
                        "lastOffset=%s, lastStableOffset=%s)",
                version(), producerId(), firstOffset(), lastOffset(), lastStableOffset());
    }

    @Override
    public boolean equals(Object any) {
        if (any instanceof AbortedTxn) {
            AbortedTxn that = (AbortedTxn) any;
            return this.buffer.equals(that.buffer);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }
}