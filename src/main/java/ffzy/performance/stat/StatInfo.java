package ffzy.performance.stat;

import ffzy.performance.report.Reporter;
import ffzy.performance.util.MathUtil;
import ffzy.performance.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangyue58 on 2018/09/13
 */
public class StatInfo {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    long statTimeMillis;

    int samplingNumRequests;
    float samplingTps;
    float samplingLatency;
    int samplingNumFailedRequests;

    int numRequests;
    float meanTps;
    float meanLatency;
    int numFailedRequests;

    public StatInfo(
            long statTimeMillis,
            int samplingNumRequests,
            float samplingTps,
            float samplingLatency,
            int samplingNumFailedRequests,
            int numRequests,
            float meanTps,
            float meanLatency,
            int numFailedRequests
    ) {
        this.statTimeMillis = statTimeMillis;

        this.samplingNumRequests = samplingNumRequests;
        this.samplingTps = samplingTps;
        this.samplingLatency = samplingLatency;
        this.samplingNumFailedRequests = samplingNumFailedRequests;

        this.numRequests = numRequests;
        this.meanTps = meanTps;
        this.meanLatency = meanLatency;
        this.numFailedRequests = numFailedRequests;
    }

    public long getStatTimeMillis() {
        return statTimeMillis;
    }

    public int getSamplingNumRequests() {
        return samplingNumRequests;
    }

    public float getSamplingTps() {
        return samplingTps;
    }

    public float getSamplingLatency() {
        return samplingLatency;
    }

    public int getSamplingNumFailedRequests() {
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

    public void dump(Reporter reporter) {
        logger.info("Sampling cycle request count:        {}", getSamplingNumRequests());
        logger.info("Sampling cycle TPS:                  {}", MathUtil.round(getSamplingTps(), 2));
        logger.info("Sampling cycle mean latency:         {}", (int)getSamplingLatency());
        logger.info("Sampling cycle failed request count: {}", getSamplingNumFailedRequests());
        logger.info("Request count:                       {}", getNumRequests());
        logger.info("Mean TPS:                            {}", MathUtil.round(getTps(), 2));
        logger.info("Mean latency:                        {}", (int)getLatency());
        logger.info("Failed request count:                {}", getNumFailedRequests());

        if (reporter != null) {
            reporter.update(
                    getStatTimeMillis(),
                    getNumRequests(), getTps(), getLatency(),
                    getSamplingNumRequests(), getSamplingTps(), getSamplingLatency(),
                    getNumFailedRequests(), getSamplingNumFailedRequests()
            );
        }
    }
}
