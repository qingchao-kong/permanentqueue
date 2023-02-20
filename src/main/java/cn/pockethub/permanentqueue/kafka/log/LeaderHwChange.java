package cn.pockethub.permanentqueue.kafka.log;

public interface LeaderHwChange {

    class Increased implements LeaderHwChange {
    }

    class Same implements LeaderHwChange {
    }

    class None implements LeaderHwChange {
    }

}
