package cn.pockethub.permanentqueue.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.function.Supplier;

public abstract class Logging {
    public static final Logger logger = LoggerFactory.getLogger(getClassNameForStatic());

    private static final Marker FatalMarker = MarkerFactory.getMarker("FATAL");


    public String logIdent(){
        return null;
    };

//    public String loggerName() {
//        return getClass().getName();
//    }

    /**
     * 利用匿名类静态方法中获取当前类名
     */
    private static final String getClassNameForStatic() {
        return new Object() {
            public String getClassName() {
                String className = this.getClass().getName();
                return className.substring(0, className.lastIndexOf('$'));
            }
        }.getClassName();
    }

    public String msgWithLogIdent(String msg) {
        if (logIdent() == null) {
            return msg;
        } else {
            return logIdent() + msg;
        }
    }

    public void trace(Supplier<String> msg) {
        logger.trace(msgWithLogIdent(msg.get()));
    }

    public void trace(Supplier<String> msg, Throwable e) {
        logger.trace(msgWithLogIdent(msg.get()), e);
    }

//    Boolean isDebugEnabled()    {
//        logger.underlying.isDebugEnabled
//    }

//    Boolean isTraceEnabled(){
//        return logger.underlying.isTraceEnabled;
//    }

    public void debug(Supplier<String> msg) {
        logger.debug(msgWithLogIdent(msg.get()));
    }

    public void debug(Supplier<String> msg, Throwable e) {
        logger.debug(msgWithLogIdent(msg.get()), e);
    }

    public void info(Supplier<String> msg) {
        logger.info(msgWithLogIdent(msg.get()));
    }

    public void info(Supplier<String> msg, Throwable e) {
        logger.info(msgWithLogIdent(msg.get()), e);
    }

    public void warn(Supplier<String> msg) {
        logger.warn(msgWithLogIdent(msg.get()));
    }

    public void warn(Supplier<String> msg, Throwable e) {
        logger.warn(msgWithLogIdent(msg.get()), e);
    }

    public void error(Supplier<String> msg) {
        logger.error(msgWithLogIdent(msg.get()));
    }

    public void error(Supplier<String> msg, Throwable e) {
        logger.error(msgWithLogIdent(msg.get()), e);
    }

    public void fatal(Supplier<String> msg) {
        logger.error(Logging.FatalMarker, msgWithLogIdent(msg.get()));
    }

    public void fatal(Supplier<String> msg, Throwable e) {
        logger.error(Logging.FatalMarker, msgWithLogIdent(msg.get()), e);
    }

}
