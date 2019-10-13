package pers.yue.test.performance.stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.common.util.ThreadUtil;

/**
 * A class that describes the final test statistic.
 *
 * Created by zhangyue182 on 2019/03/21
 */
public class FinalStatInfo {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private int numThread;
    private long duration;

    private int numRequests;
    private float meanTps;
    private float meanLatency;
    private long tp99;
    private int numFailedRequests;

    public FinalStatInfo(StatInfo statInfo) {
        this.numThread = statInfo.getNumThread();
        this.duration = statInfo.getTimeOutSecond();

        this.numRequests = statInfo.getNumRequests();
        this.meanTps = statInfo.getTps();
        this.meanLatency = statInfo.getLatency();
        this.numFailedRequests = statInfo.getNumFailedRequests();
        this.tp99 = statInfo.getTp99();
    }

    public int getNumThread() {
        return numThread;
    }

    public long getDuration() {
        return duration;
    }

    public int getNumRequests() {
        return numRequests;
    }

    public float getTps() {
        return meanTps;
    }

    public float getLatency() {
        return meanLatency;
    }

    public float getTp99() {
        return tp99;
    }

    public int getNumFailedRequests() {
        return numFailedRequests;
    }
}
