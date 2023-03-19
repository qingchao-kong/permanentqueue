package cn.pockethub.permanentqueue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

public class QueueManagerTest {

    private static final String topic = "test";

    private PermanentQueueManager permanentQueueManager;

    @BeforeEach
    public void beforeEach() throws Throwable {
        permanentQueueManager = new PermanentQueueManager();
        permanentQueueManager.startUp();
    }

    @AfterEach
    public void afterEach() throws Throwable {
        if (Objects.nonNull(permanentQueueManager)) {
            permanentQueueManager.shutDown();
        }
    }

    @Test
    public void writeMessageTest() {

    }

    @Test
    public void writeAndReadTest() throws Throwable {
        int failCount = 0;
        for (int i = 0; i < 100; i++) {
            boolean writeRes = permanentQueueManager.write(topic, Integer.toString(i).getBytes());
            if (!writeRes) {
                failCount++;
            }
        }

        Assertions.assertEquals(0, failCount);

        for (int i = 0; i < 100; i++) {
            List<PermanentQueueManager.ReadEntry> readEntries = permanentQueueManager.read(topic, 10);
            for (PermanentQueueManager.ReadEntry readEntry : readEntries) {
                permanentQueueManager.commit(topic, readEntry.getOffset());
            }
        }
    }


}
