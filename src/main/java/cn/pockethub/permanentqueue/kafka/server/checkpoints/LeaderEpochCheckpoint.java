package cn.pockethub.permanentqueue.kafka.server.checkpoints;


import cn.pockethub.permanentqueue.kafka.server.epoch.EpochEntry;

import java.util.List;

public interface LeaderEpochCheckpoint {

    void write(Iterable<EpochEntry> epochs);

    List<EpochEntry> read();
}
