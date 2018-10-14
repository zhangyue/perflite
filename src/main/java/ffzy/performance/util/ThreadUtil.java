package ffzy.performance.util;

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
            logger.info("Sleep interupted");
        }
    }


}
