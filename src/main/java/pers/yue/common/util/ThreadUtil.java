package pers.yue.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by zhangyue58 on 2017/11/21
 */
public class ThreadUtil {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public static String getClassName() {
        String className = Thread.currentThread().getStackTrace()[2].getClassName();
        String simpleClassName = className.substring(className.lastIndexOf(".") + 1);
        return simpleClassName;
    }

    public static String getMethodName() {
        String className = Thread.currentThread().getStackTrace()[2].getMethodName();
        String simpleClassName = className.substring(className.lastIndexOf(".") + 1);
        return simpleClassName;
    }

    public static void sleep(long sleepTimeInSecond) {
        sleep(sleepTimeInSecond, TimeUnit.SECONDS);
    }

    public static void sleep(long sleepTime, TimeUnit timeUnit) {
        if(sleepTime == 0) {
            return;
        }

        logger.info("Sleep {} {}.", sleepTime, timeUnit);
        try {
            Thread.sleep(timeUnit.toMillis(sleepTime));
        } catch (InterruptedException e) {
            logger.info("Sleep interrupted");
        }
    }

    public static <T> T requireNonNull(T obj) {
        if (obj == null) {
            throwRuntimeException("Null object found but non-null object required.");
        }
        return obj;
    }

    public static void throwRuntimeException(Exception e, String message) {
        try {
            throw e;
        } catch (Throwable t) {
            logger.error(message, e);
            if(e instanceof RuntimeException) {
                throw (RuntimeException)e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static void throwRuntimeException(String message) {
        logger.error(message);
        throw new RuntimeException(message);
    }

    public static void throwRuntimeException(Exception e) {
        logger.error(e.getMessage(), e);
        throw new RuntimeException(e);
    }
}
