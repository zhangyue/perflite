package ffzy.performance.runner;

import ffzy.performance.report.Reporter;
import ffzy.performance.stat.RequestResult;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

public interface PerfRunner extends Runnable {
    RequestResult doAction(int count) throws UnsupportedEncodingException, NoSuchAlgorithmException;
    RequestResult doAction(String key) throws UnsupportedEncodingException, NoSuchAlgorithmException;

    int getNumLoop();

    void setStatInterval(int samplingIntervalSeconds);
    void setReporter(Reporter reporter);
}
