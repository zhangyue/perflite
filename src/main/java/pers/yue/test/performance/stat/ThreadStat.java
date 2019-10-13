package pers.yue.test.performance.stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.common.util.LogUtil;
import pers.yue.common.util.ThreadUtil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhangyue182 on 2018/09/13
 */
public class ThreadStat {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    /**
     * Thread ID. Only for debug purpose.
     */
    private int threadId;

    /**
     * Custom log level, defined and used in corresponding client helpers.
     */
    private int logLevel;

    /**
     * Top percentile flag. NONE for no top percentile statistic (as this statistic could be huge memory consuming).
     */
    private boolean topPercentile = false;

    /**
     * Start time.
     */
    private long startTime = 0;

    /**
     * Result of requests in a real-time stat cycle.
     */
    private LinkedList<RequestResult> samplingResults = new LinkedList<>();

    /**
     * Latencies of all the requests. Only used for top-percentile.
     * If no top-percentile is required, this list will be empty (to save memory).
     */
    private Map<Long, AtomicInteger> latencies = new HashMap<>();

    /**
     * Statistic since start.
     */
    private int totalCount = 0;
    private float totalTps = 0;
    private float totalLatency = 0;
    private int totalFailCount = 0;

    /**
     * Stat of latest sampling cycle.
     */
    private float samplingTps = 0;
    private float samplingLatency = 0;
    private int samplingFailCount = 0;

    public ThreadStat(int threadId, int logLevel, boolean topPercentile) {
        this.threadId = threadId;
        this.logLevel = logLevel;
        this.topPercentile = topPercentile;
    }

    void resetSamplingCycle() {
        this.samplingResults = new LinkedList<>();
    }

    /**
     * Make thread-wide statistic.
     * @param requestResult
     */
    public void stat(RequestResult requestResult) {
        if(startTime == 0) {
            startTime = requestResult.getStartTime();
        }

        samplingResults.add(requestResult);

        updateThreadTotal(requestResult);

        if(logLevel >= LogUtil.LOG_LEVEL_V_LIGHT_STRESS) {
            logger.info("Current request latency: {} | Thread mean latency: {}",
                    requestResult.getLatency(), Math.round(totalLatency));
        }

        if (requestResult.getLatency() > 30 * 1000) {
            logger.warn("Outstanding request - latency {} seconds.", requestResult.getLatency() / 1000);
        }

        if(topPercentile) {
            if(latencies.get(requestResult.getLatency()) == null) {
                latencies.put(requestResult.getLatency(), new AtomicInteger(1));
            } else {
                latencies.get(requestResult.getLatency()).incrementAndGet();
            }
        }
    }

    private void updateThreadTotal(RequestResult requestResult) {
        totalLatency = (totalLatency * totalCount + requestResult.getLatency()) / ++totalCount;
        totalTps = totalCount / ((float)(requestResult.getStartTime() + requestResult.getLatency() - startTime) / 1000);
        if(!requestResult.isSuccess()) {
            totalFailCount++;
        }
    }

    private int getSamplingLatencySum() {
        int samplingLatencySum = 0;
        Iterator<RequestResult> iterator = samplingResults.iterator();
        while (iterator.hasNext()) {
            samplingLatencySum += iterator.next().getLatency();
        }
        return samplingLatencySum;
    }

    private int calculateSamplingFailCount() {
        int samplingFailCount = 0;
        Iterator<RequestResult> iterator = samplingResults.iterator();
        while (iterator.hasNext()) {
            if(!iterator.next().isSuccess()) {
                samplingFailCount++;
            }
        }
        return samplingFailCount;
    }

    LinkedList<RequestResult> getSamplingResults() {
        return samplingResults;
    }

    int getTotalCount() {
        return totalCount;
    }

    float getTotalLatency() {
        return totalLatency;
    }

    int getTotalFailCount() {
        return totalFailCount;
    }

    public float getSamplingTps() {
        RequestResult firstRequestResult = samplingResults.getFirst();
        RequestResult lastRequestResult = samplingResults.getLast();
        return samplingResults.size() / ((float)((lastRequestResult.getStartTime() + lastRequestResult.getLatency()) - firstRequestResult.getStartTime()) / 1000);
    }

    Float getSamplingLatency() {
        return (float) getSamplingLatencySum() / samplingResults.size();
    }

    int getSamplingFailCount() {
        return calculateSamplingFailCount();
    }

    Map<Long, AtomicInteger> getLatencies() {
        return latencies;
    }

    @Override
    public String toString() {
        return "startTime: " + startTime + "; totalCount: " + totalCount + "; totalTps: " + totalTps
                + "; totalLatency: " + totalLatency + "; totalFailCount: " + totalFailCount
                + "; samplingTps: " + samplingTps + "; samplingLatency: " + samplingLatency
                + "; samplingFailCount: " + samplingFailCount;
    }

    public static void main(String[] args) {
        test();
    }

    private static void test() {
        ThreadStat threadStat = new ThreadStat(1, 1, false);
        long start = System.currentTimeMillis();
        long latency;
        latency = 100; threadStat.stat(new RequestResult(start, latency, true)); start += latency; System.out.println(threadStat.toString());
        latency = 80; threadStat.stat(new RequestResult(start, latency, false)); start += latency; System.out.println(threadStat.toString());
        latency = 70; threadStat.stat(new RequestResult(start, latency, true)); start += latency; System.out.println(threadStat.toString());
        latency = 70; threadStat.stat(new RequestResult(start, latency, false)); start += latency; System.out.println(threadStat.toString());
        latency = 75; threadStat.stat(new RequestResult(start, latency, true)); start += latency; System.out.println(threadStat.toString());
        latency = 80; threadStat.stat(new RequestResult(start, latency, true)); start += latency; System.out.println(threadStat.toString());
        latency = 90; threadStat.stat(new RequestResult(start, latency, true)); start += latency; System.out.println(threadStat.toString());
        latency = 75; threadStat.stat(new RequestResult(start, latency, true)); start += latency; System.out.println(threadStat.toString());
        latency = 79; threadStat.stat(new RequestResult(start, latency, true)); start += latency; System.out.println(threadStat.toString());
        latency = 90; threadStat.stat(new RequestResult(start, latency, true)); start += latency; System.out.println(threadStat.toString());
        latency = 100; threadStat.stat(new RequestResult(start, latency, true)); start += latency; System.out.println(threadStat.toString());
        latency = 110; threadStat.stat(new RequestResult(start, latency, true)); start += latency; System.out.println(threadStat.toString());
        latency = 90; threadStat.stat(new RequestResult(start, latency, true)); start += latency; System.out.println(threadStat.toString());
        latency = 100; threadStat.stat(new RequestResult(start, latency, true)); start += latency; System.out.println(threadStat.toString());
        latency = 120; threadStat.stat(new RequestResult(start, latency, true)); start += latency; System.out.println(threadStat.toString());
    }
}
