package pers.yue.test.performance.runner;

import pers.yue.stoppable.Stoppable;
import pers.yue.test.performance.report.Reporter;
import pers.yue.test.performance.stat.RequestResult;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

/**
 * The interface that defines the basic operations of performance test runner.
 *
 * Created by zhangyue182 on 2017/11/29
 */
public interface PerfRunner extends Runnable, Stoppable {
    RequestResult doAction() throws UnsupportedEncodingException, NoSuchAlgorithmException;
    RequestResult doAction(String key) throws UnsupportedEncodingException, NoSuchAlgorithmException;

    int getNumLoop();

    void setStatInterval(int samplingIntervalSeconds);
    void setReporter(Reporter reporter);
    void setLogLevel(int logLevel);
}
