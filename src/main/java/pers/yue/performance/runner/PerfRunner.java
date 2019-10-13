package pers.yue.performance.runner;

import pers.yue.performance.report.Reporter;
import pers.yue.performance.stat.RequestResult;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

public interface PerfRunner extends Runnable {
    RequestResult doAction(int count) throws UnsupportedEncodingException, NoSuchAlgorithmException;
    RequestResult doAction(String key) throws UnsupportedEncodingException, NoSuchAlgorithmException;

    int getNumLoop();

    void setStatInterval(int samplingIntervalSeconds);
    void setReporter(Reporter reporter);
}
