package cn.pockethub.permanentqueue;

import cn.pockethub.permanentqueue.kafka.log.LogConfig;
import cn.pockethub.permanentqueue.kafka.log.LogManager;
import cn.pockethub.permanentqueue.kafka.log.UnifiedLog;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.Getter;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

public class PermanentQueueManager extends AbstractIdleService {
    private static final Logger LOG = LoggerFactory.getLogger(PermanentQueueManager.class);

    public static final long DEFAULT_COMMITTED_OFFSET = Long.MIN_VALUE;

    private static final String newLine=System.getProperty("line.separator");

    private LogManager logManager;

    private final ConcurrentMap<String, PermanentQueue> queueHolder = new ConcurrentHashMap<>();
    private final Object createQueueLock = new Object();

    @Getter
    private final File logDir;
    private final File committedReadOffsetFile;

    private final OffsetFileFlusher offsetFlusher;

    @Getter
    private volatile boolean shuttingDown;

    public PermanentQueueManager(LogManager logManager, File logDir) {
        this.logManager = logManager;
        this.logDir = logDir;

        committedReadOffsetFile = new File(logDir, "permanentqueue-committed-read-offset");
        try {
            committedReadOffsetFile.createNewFile();
        } catch (IOException e) {
            LOG.error("Cannot access offset file: {}", e.getMessage());
            final AccessDeniedException accessDeniedException = new AccessDeniedException(committedReadOffsetFile.getAbsolutePath(), null, e.getMessage());
            throw new RuntimeException(accessDeniedException);
        }

        //offset flusher
        offsetFlusher = new OffsetFileFlusher();
    }

    /**
     * Start the background threads to flush logs and do log cleanup
     */
    @Override
    public void startUp() throws Exception {
        logManager.getScheduler().startup();
        logManager.startup(new HashSet<>());
        logManager.getScheduler().schedule("PermanentQueue-offset-flusher", offsetFlusher, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void shutDown() throws Exception {
        LOG.debug("Shutting down PermanentQueueManager!");
        shuttingDown = true;
        try {
            logManager.shutdown();
        } catch (Throwable throwable) {
            LOG.error("Shut donw PermanentQueueManager error", throwable);
        }
        try {
            logManager.getScheduler().shutdown();
        } catch (InterruptedException e) {
            LOG.error("Shut down scheduler error", e);
        }
        // final flush
        offsetFlusher.run();
    }

    public PermanentQueue getOrCreatePermanentQueue(String queueName) throws Throwable {
        if (!queueHolder.containsKey(queueName)) {
            synchronized (createQueueLock) {
                if (!queueHolder.containsKey(queueName)) {
                    UnifiedLog unifiedLog = logManager.getOrCreateLog(new TopicPartition(queueName, 0), Optional.empty());
                    long committedOffset = DEFAULT_COMMITTED_OFFSET;
                    long nextReadOffset = 0L;
                    try {
                        ImmutableList<String> lines = Files.asCharSource(committedReadOffsetFile, StandardCharsets.UTF_8).readLines();
                        // the file contains the last offset has successfully processed.
                        // thus the nextReadOffset is one beyond that number
                        for (String line : lines) {
                            if (StringUtils.isBlank(line)) {
                                continue;
                            }
                            //queueName:offset
                            String[] split = line.split(":");
                            if (StringUtils.equals(split[0], queueName)) {
                                committedOffset = Long.parseLong(split[1]);
                                nextReadOffset = committedOffset + 1;
                                break;
                            }
                        }
                    } catch (IOException e) {
                        LOG.error("Cannot access offset file: {}", e.getMessage());
                        final AccessDeniedException accessDeniedException = new AccessDeniedException(committedReadOffsetFile.getAbsolutePath(), null, e.getMessage());
                        throw new RuntimeException(accessDeniedException);
                    }
                    queueHolder.put(queueName, new PermanentQueue(queueName, this, unifiedLog, committedOffset, nextReadOffset));
                }
            }
        }
        return queueHolder.get(queueName);
    }

    public class OffsetFileFlusher implements Runnable {

        @Override
        public void run() {
            // Do not write the file if committedOffset has never been updated.
            Map<String,Long> committedOffsets=new TreeMap<>();
            for (Map.Entry<String, PermanentQueue> entry : queueHolder.entrySet()) {
                String queueName = entry.getKey();
                PermanentQueue queue = entry.getValue();
                if (queue.getCommittedOffset().get() != DEFAULT_COMMITTED_OFFSET) {
                    committedOffsets.put(queueName, queue.getCommittedOffset().get());
                }
            }

            if (MapUtils.isNotEmpty(committedOffsets)) {
                try (final FileOutputStream fos = new FileOutputStream(committedReadOffsetFile)) {
                    for (Map.Entry<String, Long> entry :committedOffsets.entrySet()) {
                        fos.write(String.format("%s:%s",entry.getKey(),entry.getValue()).getBytes(StandardCharsets.UTF_8));
                        fos.write(newLine.getBytes(StandardCharsets.UTF_8));
                    }
                    // flush stream
                    fos.flush();
                    // actually sync to disk
                    fos.getFD().sync();
                } catch (SyncFailedException e) {
                    LOG.error("Cannot sync " + committedReadOffsetFile.getAbsolutePath() + " to disk. Continuing anyway," +
                            " but there is no guarantee that the file has been written.", e);
                } catch (IOException e) {
                    LOG.error("Cannot write " + committedReadOffsetFile.getAbsolutePath() + " to disk.", e);
                }
            }



        }
    }
}
