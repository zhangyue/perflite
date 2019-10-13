package pers.yue.test.performance.stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.test.performance.report.Reporter;
import pers.yue.util.MathUtil;
import pers.yue.util.ThreadUtil;

/**
 * A class that describes test statistic metrics in a sampling interval.
 *
 * Created by zhangyue182 on 2018/09/13
 */
public class StatInfo {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private int numThread;
    private int numLoop;
    private long timeOutSecond;

    private int logLevel;

    private long statTimeMillis;

    private int samplingNumRequests;
    private float samplingTps;
    private float samplingLatency;
    private int samplingNumFailedRequests;

    private int numRequests;
    private float meanTps;
    private float meanLatency;
    private int numFailedRequests;

    private long tp90, tp95, tp99;

    public StatInfo() {

    }

    StatInfo(
            int logLevel,
            long statTimeMillis,
            int samplingNumRequests,
            float samplingTps,
            float samplingLatency,
            int samplingNumFailedRequests,
            int numRequests,
            float meanTps,
            float meanLatency,
            int numFailedRequests,
            long tp90,
            long tp95,
            long tp99
    ) {
        this.logLevel = logLevel;
        this.statTimeMillis = statTimeMillis;

        this.samplingNumRequests = samplingNumRequests;
        this.samplingTps = samplingTps;
        this.samplingLatency = samplingLatency;
        this.samplingNumFailedRequests = samplingNumFailedRequests;

        this.numRequests = numRequests;
        this.meanTps = meanTps;
        this.meanLatency = meanLatency;
        this.numFailedRequests = numFailedRequests;

        this.tp90 = tp90;
        this.tp95 = tp95;
        this.tp99 = tp99;
    }

    public int getLogLevel() {
        return logLevel;
    }

    public long getStatTimeMillis() {
        return statTimeMillis;
    }

    int getNumThread() {
        return numThread;
    }

    public int getNumLoop() {
        return numLoop;
    }

    long getTimeOutSecond() {
        return timeOutSecond;
    }

    public StatInfo withParameters(int numLoop, int numThread, long timeOutSecond) {
        this.numLoop = numLoop;
        this.numThread = numThread;
        this.timeOutSecond = timeOutSecond;
        return this;
    }

    private int getSamplingNumRequests() {
        return samplingNumRequests;
    }

    private float getSamplingTps() {
        return samplingTps;
    }

    private float getSamplingLatency() {
        return samplingLatency;
    }

    private int getSamplingNumFailedRequests() {
        return samplingNumFailedRequests;
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

    public int getNumFailedRequests() {
        return numFailedRequests;
    }

    public long getTp90() {
        return tp90;
    }

    public long getTp95() {
        return tp95;
    }

    public long getTp99() {
        return tp99;
    }

    public void dump(Reporter reporter) {
        if(logLevel > 0) {
            logger.info("Sampling cycle request count:        {}", getSamplingNumRequests());
            logger.info("Sampling cycle TPS:                  {}", MathUtil.round(getSamplingTps(), 2));
            logger.info("Sampling cycle mean latency:         {}", (int) getSamplingLatency());
            logger.info("Sampling cycle failed request count: {}", getSamplingNumFailedRequests());
            logger.info("Request count:                       {}", getNumRequests());
            logger.info("Mean TPS:                            {}", MathUtil.round(getTps(), 2));
            logger.info("Mean latency:                        {}", (int) getLatency());
            logger.info("Failed request count:                {}", getNumFailedRequests());
            logger.info("TP90:                                {}", getTp90());
            logger.info("TP95:                                {}", getTp95());
            logger.info("TP99:                                {}", getTp99());
        }

        if (reporter != null) {
            reporter.update(
                    getStatTimeMillis(),
                    getNumRequests(), getTps(), getLatency(),
                    getSamplingNumRequests(), getSamplingTps(), getSamplingLatency(),
                    getNumFailedRequests(), getSamplingNumFailedRequests(),
                    getTp90(), getTp95(), getTp99()
            );
        }
    }
}
