package pers.yue.exceptions.runtime;

/**
 * Created by zhangyue182 on 2019/05/10
 */
public class TestRunException extends RuntimeException {
    public TestRunException(Throwable t) {
        super(t);
    }

    public TestRunException(String message, Throwable t) {
        super(message, t);
    }

    public TestRunException(String message) {
        super(message);
    }
}
