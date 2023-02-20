package cn.pockethub.permanentqueue.kafka.utils;

import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor
 * <p>
 * It has a pool of kafka-scheduler- threads that do the actual work.
 */
//@threadsafe
public class KafkaScheduler implements Scheduler {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaScheduler.class);

    private static final String THREAD_NAME_PREFIX = "kafka-scheduler-";

    private Integer threads;
    private String threadNamePrefix;
    private Boolean daemon;

    private ScheduledThreadPoolExecutor executor = null;
    private AtomicInteger schedulerThreadId = new AtomicInteger(0);

    public KafkaScheduler(Integer threads) {
        this(threads, THREAD_NAME_PREFIX, true);
    }

    /**
     * @param threads          The number of threads in the thread pool
     * @param threadNamePrefix The name to use for scheduler threads. This prefix will have a number appended to it.
     * @param daemon           If true the scheduler threads will be "daemon" threads and will not block jvm shutdown.
     */
    public KafkaScheduler(Integer threads,
                          String threadNamePrefix,
                          Boolean daemon) {
        this.threads = threads;
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
    }

    @Override
    public void startup() {
        LOG.debug("Initializing task scheduler.");
        synchronized (this) {
            if (isStarted()) {
                throw new IllegalStateException("This scheduler has already been started!");
            }
            executor = new ScheduledThreadPoolExecutor(threads);
            executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            executor.setRemoveOnCancelPolicy(true);
            executor.setThreadFactory(runnable ->
                    new KafkaThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), runnable, daemon)
            );
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        LOG.debug("Shutting down task scheduler.");
        // We use the local variable to avoid NullPointerException if another thread shuts down scheduler at same time.
        ScheduledThreadPoolExecutor cachedExecutor = this.executor;
        if (cachedExecutor != null) {
            synchronized (this) {
                cachedExecutor.shutdown();
                this.executor = null;
            }
            cachedExecutor.awaitTermination(1, TimeUnit.DAYS);
        }
    }

    public void scheduleOnce(String name, Runnable fun) {
        schedule(name, fun, 0L, -1L, TimeUnit.MILLISECONDS);
    }

    @Override
    public ScheduledFuture schedule(String name, Runnable fun, long delay, long period, TimeUnit unit) {
        LOG.debug("Scheduling task {} with initial delay {} ms and period {} ms."
                , name, TimeUnit.MILLISECONDS.convert(delay, unit), TimeUnit.MILLISECONDS.convert(period, unit));
        synchronized (this) {
            if (isStarted()) {
                Runnable runnable = () -> {
                    try {
                        LOG.trace("Beginning execution of scheduled task '{}'.", name);
                        fun.run();
                    } catch (Throwable t) {
                        LOG.error("Uncaught exception in scheduled task '{}'", name, t);
                    } finally {
                        LOG.trace("Completed execution of scheduled task '{}'.", name);
                    }
                };
                if (period >= 0) {
                    return executor.scheduleAtFixedRate(runnable, delay, period, unit);
                } else {
                    return executor.schedule(runnable, delay, unit);
                }
            } else {
                LOG.info("Kafka scheduler is not running at the time task '{}' is scheduled. The task is ignored.", name);
                return new NoOpScheduledFutureTask();
            }
        }
    }

    /**
     * Package private for testing.
     */
    protected Boolean taskRunning(ScheduledFuture task) {
        return executor.getQueue().contains(task);
    }

    public void resizeThreadPool(Integer newSize) {
        executor.setCorePoolSize(newSize);
    }

    public Boolean isStarted() {
        synchronized (this) {
            return executor != null;
        }
    }
}

