package cn.pockethub.permanentqueue.kafka.utils;

import lombok.Getter;
import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.utils.Exit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Getter
public abstract class ShutdownableThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ShutdownableThread.class);

    private final boolean isInterruptible;

    private final String logIdent;

    private CountDownLatch shutdownInitiated = new CountDownLatch(1);
    private CountDownLatch shutdownComplete = new CountDownLatch(1);
    private volatile Boolean isStarted = false;

    public ShutdownableThread(final String name) {
        this(name, true);
    }

    public ShutdownableThread(final String name, final boolean isInterruptible) {
        super(name);
        this.isInterruptible = isInterruptible;

        this.logIdent="[" + name + "]: ";

        setDaemon(false);
    }

    public void shutdown() throws InterruptedException {
        initiateShutdown();
        awaitShutdown();
    }

    public Boolean isShutdownInitiated(){
        return shutdownInitiated.getCount() == 0;
    }

    public Boolean isShutdownComplete(){
        return shutdownComplete.getCount() == 0;
    }

    /**
     * @return true if there has been an unexpected error and the thread shut down
     */
    // mind that run() might set both when we're shutting down the broker
    // but the return value of this function at that point wouldn't matter
    public Boolean isThreadFailed(){
        return isShutdownComplete() && !isShutdownInitiated();
    }

    public Boolean initiateShutdown(){
        synchronized (this){
            if (isRunning()) {
                LOG.info("Shutting down");
                shutdownInitiated.countDown();
                if (isInterruptible) {
                    interrupt();
                }
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * After calling initiateShutdown(), use this API to wait until the shutdown is complete
     */
    public void awaitShutdown()throws InterruptedException{
        if (!isShutdownInitiated()) {
            throw new IllegalStateException("initiateShutdown() was not called before awaitShutdown()");
        }
        else {
            if (isStarted) {
                shutdownComplete.await();
            }
            LOG.info("Shutdown completed");
        }
    }

    /**
     *  Causes the current thread to wait until the shutdown is initiated,
     *  or the specified waiting time elapses.
     *
     * @param timeout
     * @param unit
     */
    public void pause(Long timeout,TimeUnit unit) throws InterruptedException{
        if (shutdownInitiated.await(timeout, unit)) {
            LOG.trace("shutdownInitiated latch count reached zero. Shutdown called.");
        }
    }

    public abstract void doWork() throws Throwable;

    @Override
     public void run(){
        isStarted = true;
        LOG.info("Starting");
        try {
            while (isRunning()) {
                doWork();
            }
        } catch (FatalExitError e){
            shutdownInitiated.countDown();
            shutdownComplete.countDown();
            LOG.info("Stopped");
            Exit.exit(e.statusCode());
        }catch (Throwable e){
            if (isRunning()) {
                LOG.error("Error due to", e);
            }
        }finally {
            shutdownComplete.countDown();
        }
        LOG.info("Stopped");
    }

    public Boolean isRunning(){
        return !isShutdownInitiated();
    }
}
