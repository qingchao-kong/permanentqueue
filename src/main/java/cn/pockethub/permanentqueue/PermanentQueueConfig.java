package cn.pockethub.permanentqueue;

public class PermanentQueueConfig {

    /**
     * The directory in which the data is kept
     */
    private String storePath = null;

    private PermanentQueueConfig() {
    }

    public String getStorePath() {
        return storePath;
    }

    public static class Builder {

        private String storePath = null;

        public Builder() {
        }

        public Builder storePath(String storePath) {
            this.storePath = storePath;
            return this;
        }

        public PermanentQueueConfig build() {
            PermanentQueueConfig config = new PermanentQueueConfig();
            config.storePath = storePath;
            return config;
        }
    }

    public int getFlushConsumerOffsetInterval() {
        return 1000;
    }
}
