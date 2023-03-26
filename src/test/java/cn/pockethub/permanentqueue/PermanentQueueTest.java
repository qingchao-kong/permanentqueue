package cn.pockethub.permanentqueue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Objects;

public class PermanentQueueTest {

    private static final String topic = "test";

    private PermanentQueue permanentQueue;

    @BeforeEach
    public void beforeEach() throws Throwable {
        String baseDir = System.getProperty("java.io.tmpdir") + File.separator + "store-kqc";
        PermanentQueueConfig config = new PermanentQueueConfig.Builder()
                .storePath(baseDir)
                .build();
        permanentQueue = new PermanentQueue(config);
        permanentQueue.startUp();
    }

    @AfterEach
    public void afterEach() throws Throwable {
        if (Objects.nonNull(permanentQueue)) {
            permanentQueue.shutDown();
        }
    }

    @Test
    public void writeMessageTest() {

    }

    @Test
    public void writeAndReadTest() throws Throwable {
        int failCount = 0;
        for (int i = 0; i < 100; i++) {
            long offset = permanentQueue.write(topic, Integer.toString(i).getBytes());
            if (offset<0) {
                failCount++;
            }
        }

        Assertions.assertEquals(0, failCount);

        for (int i = 0; i < 100; i++) {
            List<Queue.ReadEntry> readEntries = permanentQueue.read(topic, 10);
            for (Queue.ReadEntry readEntry : readEntries) {
                permanentQueue.commit(topic, readEntry.getOffset());
            }
        }
    }


}
