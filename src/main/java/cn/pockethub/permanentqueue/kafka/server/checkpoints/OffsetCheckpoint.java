package cn.pockethub.permanentqueue.kafka.server.checkpoints;

import cn.pockethub.permanentqueue.kafka.server.epoch.EpochEntry;

import java.util.List;

public interface OffsetCheckpoint {

    void write(List<EpochEntry> epochs);

    List<EpochEntry> read();
}
