package pers.yue.test.performance.stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.common.util.ThreadUtil;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhangyue182 on 2018/09/14
 */
public class GlobalStat {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private int logLevel;
    private TreeMap<Long, AtomicInteger> globalLatencies = new TreeMap<>();
    private Map<Integer, ThreadStat> threadStats;
    private long testStartMillis;
    private long lastStatMillis;
    private long now;

    public GlobalStat(int logLevel, Map<Integer, ThreadStat> threadStats, long testStartMillis, long lastStatMillis) {
        this.logLevel = logLevel;
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

        long tp90 = 0, tp95 = 0, tp99 = 0;

        if(prepareCalculateTopPercentile(threadStats)) {
            tp90 = calculateTopPercentile(90);
            tp95 = calculateTopPercentile(95);
            tp99 = calculateTopPercentile(99);
        }

        return new StatInfo(logLevel, now,
                samplingCount, samplingTps, samplingLatency, samplingFailCount,
                totalCount, totalTps, totalLatency, totalFailCount,
                tp90, tp95, tp99
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
            if(threadStat.getSamplingLatency().isNaN()) {
                logger.warn("NaN detected when get sampling latency of certain thread. " +
                        "This means that the thread has no request completed during this sampling interval. " +
                        "If this happens a lot, please consider enlarge the samplingInterval parameter.");
                continue;
            }

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

    private boolean prepareCalculateTopPercentile(Map<Integer, ThreadStat> threadStats) {
        if(threadStats.get(0).getLatencies().size() == 0
                && threadStats.get(threadStats.size() - 1).getLatencies().size() == 0) {
            return false;
        }
        consolidateThreadStats(threadStats);
        return true;
    }

    private long calculateTopPercentile(int tpPercent) {
        int totalNumRequests = getValueSum(globalLatencies);
        int scannedNumRequests = 0;

        for(Map.Entry<Long, AtomicInteger> entry : globalLatencies.entrySet()) {
            scannedNumRequests += entry.getValue().get();
            if(scannedNumRequests >= totalNumRequests * tpPercent / 100) {
                return entry.getKey();
            }
        }
        return globalLatencies.lastKey();
    }

    private void consolidateThreadStats(Map<Integer, ThreadStat> threadStats) {
        for(ThreadStat threadStat : threadStats.values()) {
            for(Map.Entry<Long, AtomicInteger> entry : threadStat.getLatencies().entrySet()) {
                if(globalLatencies.get(entry.getKey()) == null) {
                    globalLatencies.put(entry.getKey(), new AtomicInteger(entry.getValue().get()));
                } else {
                    globalLatencies.get(entry.getKey()).addAndGet(entry.getValue().get());
                }
            }
        }
    }

    private int getValueSum(Map<Long, AtomicInteger> map) {
        int sum = 0;
        for(Map.Entry<Long, AtomicInteger> entry : map.entrySet()) {
            sum += entry.getValue().get();
        }
        return sum;
    }

    private void resetThreadSamplingCycle(Map<Integer, ThreadStat> threadStats) {
        for(ThreadStat threadStat : threadStats.values()) {
            threadStat.resetSamplingCycle();
        }
    }
}
