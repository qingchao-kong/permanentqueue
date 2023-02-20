package cn.pockethub.permanentqueue.kafka.server.epoch;

import lombok.Getter;

@Getter
public class EpochEntry {
    private Integer epoch;
    private Long startOffset;

    public EpochEntry(Integer epoch,Long startOffset){
        this.epoch=epoch;
        this.startOffset=startOffset;
    }

    @Override
    public String toString(){
        return String.format("EpochEntry(epoch=%s, startOffset=%s)", epoch, startOffset);
    }
}
