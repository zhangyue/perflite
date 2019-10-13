package pers.yue.test.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.exceptions.runtime.TestRunException;
import pers.yue.common.util.ThreadUtil;

/**
 * Created by Zhang Yue on 5/12/2019
 */

/**
 * Methods in this class convert checked exception to run-time exception, and print necessary error message and
 * stack trace to log.
 */
public class TestUtilBase {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    @FunctionalInterface
    public interface TestUtilMethod<R, P> {
        R execute(P param) throws Exception;
    }

    @FunctionalInterface
    public interface TestUtilMethodWithLogLevel<R, P1, P2> {
        R execute(P1 param1, P2 param2) throws Exception;
    }

    @FunctionalInterface
    public interface TestUtilMethodWithRange<R, P1, P2, P3> {
        R execute(P1 param1, P2 param2, P3 param3) throws Exception;
    }

    static <R, P> R execute(String methodName, TestUtilMethod<R, P> method, P param) {
        try {
            return method.execute(param);
        } catch (Exception e) {
            logger.error("{} when {}({}).", e.getClass().getSimpleName(), methodName, param, e);
            throw new TestRunException(e);
        }
    }

    static <R, P1, P2> R execute(String methodName, TestUtilMethodWithLogLevel<R, P1, P2> method, P1 param1, P2 param2) {
        try {
            return method.execute(param1, param2);
        } catch (Exception e) {
            logger.error("{} when {}({}, {}).", e.getClass().getSimpleName(), methodName, param1, param2, e);
            throw new TestRunException(e);
        }
    }

    static <R, P1, P2, P3> R execute(String methodName, TestUtilMethodWithRange<R, P1, P2, P3> method, P1 param1, P2 param2, P3 param3) {
        try {
            return method.execute(param1, param2, param3);
        } catch (Exception e) {
            logger.error("{} when {}({}, {}, {}).", e.getClass().getSimpleName(), methodName, param1, param2, param3, e);
            throw new TestRunException(e);
        }
    }

}
