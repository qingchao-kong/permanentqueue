package cn.pockethub.permanentqueue.kafka.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/*
 * LogDirFailureChannel allows an external thread to block waiting for new offline log dirs.
 *
 * There should be a single instance of LogDirFailureChannel accessible by any class that does disk-IO operation.
 * If IOException is encountered while accessing a log directory, the corresponding class can add the log directory name
 * to the LogDirFailureChannel using maybeAddOfflineLogDir(). Each log directory will be added only once. After a log
 * directory is added for the first time, a thread which is blocked waiting for new offline log directories
 * can take the name of the new offline log directory out of the LogDirFailureChannel and handle the log failure properly.
 * An offline log directory will stay offline until the broker is restarted.
 *
 */
public class LogDirFailureChannel {

    private static final Logger LOG = LoggerFactory.getLogger(LogDirFailureChannel.class);

    private final Integer logDirNum;

    private final ConcurrentHashMap<String,String> offlineLogDirs=new ConcurrentHashMap<>();
    private final ArrayBlockingQueue<String> offlineLogDirQueue;


    public LogDirFailureChannel(Integer logDirNum){
        this.logDirNum=logDirNum;
        this.offlineLogDirQueue=new ArrayBlockingQueue(logDirNum);
    }


    public Boolean hasOfflineLogDir(String logDir) {
       return offlineLogDirs.containsKey(logDir);
    }

    /*
     * If the given logDir is not already offline, add it to the
     * set of offline log dirs and enqueue it to the logDirFailureEvent queue
     */
    public void maybeAddOfflineLogDir(String logDir , String msg, IOException e ){
        LOG.error(msg, e);
        if (offlineLogDirs.putIfAbsent(logDir, logDir) == null) {
            offlineLogDirQueue.add(logDir);
        }
    }

    /*
     * Get the next offline log dir from logDirFailureEvent queue.
     * The method will wait if necessary until a new offline log directory becomes available
     */
    public String takeNextOfflineLogDir()throws InterruptedException{
        return offlineLogDirQueue.take();
    }

}
