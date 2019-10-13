package pers.yue.performance.stat;

import pers.yue.performance.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by zhangyue58 on 2018/09/14
 */
public class GlobalStat {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private Map<Integer, ThreadStat> threadStats;
    private long testStartMillis;
    private long lastStatMillis;
    private long now;

    public GlobalStat(Map<Integer, ThreadStat> threadStats, long testStartMillis, long lastStatMillis) {
        this.threadStats = threadStats;
        this.testStartMillis = testStartMillis;
        this.lastStatMillis = lastStatMillis;
        now = System.currentTimeMillis();
    }

    /**
     * Make global statistic.
     * @return
     */
    public StatInfo stat() {
        int samplingCount = calculateSamplingCount(threadStats);
        float samplingTps = calculateSamplingTps(threadStats);
        float samplingLatency = calculateSamplingLatency(threadStats);
        int samplingFailCount = calculateSamplingFailCount(threadStats);

        resetThreadSamplingCycle(threadStats);

        int totalCount = calculateTotalCount(threadStats);
        float totalTps = calculateTotalTps(totalCount);
        float totalLatency = calculateTotalLatency(threadStats);
        int totalFailCount = calculateTotalFailCount(threadStats);

        return new StatInfo(now,
                samplingCount, samplingTps, samplingLatency, samplingFailCount,
                totalCount, totalTps, totalLatency, totalFailCount
        );
    }

    private int calculateTotalCount(Map<Integer, ThreadStat> threadStats) {
        int totalCount = 0;
        for(ThreadStat threadStat : threadStats.values()) {
            totalCount += threadStat.getTotalCount();
        }
        return totalCount;
    }

    private float calculateTotalTps(int totalCount) {
        return (float)totalCount / ((float)(now - testStartMillis) / 1000);
    }

    private float calculateTotalLatency(Map<Integer, ThreadStat> threadStats) {
        float totalLatencySum = 0;
        float totalNumRequestsSum = 0;
        for(ThreadStat threadStat : threadStats.values()) {
            totalLatencySum += threadStat.getTotalLatency() * threadStat.getTotalCount();
            totalNumRequestsSum += threadStat.getTotalCount();
        }
        return totalLatencySum / totalNumRequestsSum;
    }

    private int calculateTotalFailCount(Map<Integer, ThreadStat> threadStats) {
        int totalFailCount = 0;
        for(ThreadStat threadStat : threadStats.values()) {
            totalFailCount += threadStat.getTotalFailCount();
        }
        return totalFailCount;
    }

    private int calculateSamplingCount(Map<Integer, ThreadStat> threadStats) {
        int samplingCount = 0;
        for(ThreadStat threadStat : threadStats.values()) {
            samplingCount += threadStat.getSamplingResults().size();
        }
        return samplingCount;
    }

    private float calculateSamplingTps(Map<Integer, ThreadStat> threadStats) {
        float cycleMillis = now - lastStatMillis;

        int samplingCountSum = calculateSamplingCount(threadStats);

        logger.debug("samplingCountSum: {}", samplingCountSum);
        logger.debug("cycleMillis: {}", cycleMillis);
        return samplingCountSum / (cycleMillis / 1000);
    }

    private float calculateSamplingLatency(Map<Integer, ThreadStat> threadStats) {
        float samplngLatencySum = 0;
        float samplingNumRequestsSum = 0;
        for(ThreadStat threadStat : threadStats.values()) {
            samplngLatencySum += threadStat.getSamplingLatency() * threadStat.getSamplingResults().size();
            samplingNumRequestsSum += threadStat.getSamplingResults().size();
        }
        return samplngLatencySum / samplingNumRequestsSum;
    }

    private int calculateSamplingFailCount(Map<Integer, ThreadStat> threadStats) {
        int samplingFailCount = 0;
        for(ThreadStat threadStat : threadStats.values()) {
            samplingFailCount += threadStat.getSamplingFailCount();
        }
        return samplingFailCount;
    }

    private void resetThreadSamplingCycle(Map<Integer, ThreadStat> threadStats) {
        for(ThreadStat threadStat : threadStats.values()) {
            threadStat.resetSamplingCycle();
        }
    }
}
