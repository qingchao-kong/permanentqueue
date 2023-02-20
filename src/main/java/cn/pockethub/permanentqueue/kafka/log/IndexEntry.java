package cn.pockethub.permanentqueue.kafka.log;

public interface IndexEntry {
    // We always use Long for both key and value to avoid boxing.
    Long indexKey();

    Long indexValue();
}
