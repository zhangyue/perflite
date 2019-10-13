package pers.yue.performance.stat;

import pers.yue.performance.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/**
 * Created by zhangyue58 on 2018/09/13
 */
public class ThreadStat {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    /**
     * Start time.
     */
    private long startTime = 0;

    /**
     * Result of requests in a real-time stat cycle.
     */
    private LinkedList<RequestResult> samplingResults;

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

    public ThreadStat() {
        this.samplingResults = new LinkedList<>();
    }

    public void resetSamplingCycle() {
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

        logger.info("Current request latency: {}", requestResult.getLatency());
        logger.info("Thread mean latency:     {}", Math.round(totalLatency));

        if (requestResult.getLatency() > 30 * 1000) {
            logger.warn("Outstanding request - latency {} seconds.", requestResult.getLatency() / 1000);
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
        for(RequestResult requestResult : samplingResults) {
            samplingLatencySum += requestResult.getLatency();
        }
        return samplingLatencySum;
    }

    private int calculateSamplingFailCount() {
        int samplingFailCount = 0;
        for(RequestResult requestResult : samplingResults) {
            if(!requestResult.isSuccess()) {
                samplingFailCount++;
            }
        }
        return samplingFailCount;
    }

    public LinkedList<RequestResult> getSamplingResults() {
        return samplingResults;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public float getTotalLatency() {
        return totalLatency;
    }

    public int getTotalFailCount() {
        return totalFailCount;
    }

    public float getSamplingTps() {
        RequestResult firstRequestResult = samplingResults.getFirst();
        RequestResult lastRequestResult = samplingResults.getLast();
        return samplingResults.size() / ((float)((lastRequestResult.getStartTime() + lastRequestResult.getLatency()) - firstRequestResult.getStartTime()) / 1000);
    }

    public float getSamplingLatency() {
        return (float) getSamplingLatencySum() / samplingResults.size();
    }

    public int getSamplingFailCount() {
        return calculateSamplingFailCount();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("startTime: ").append(startTime).append("; ");
        sb.append("totalCount: ").append(totalCount).append("; ");
        sb.append("totalTps: ").append(totalTps).append("; ");
        sb.append("totalLatency: ").append(totalLatency).append("; ");
        sb.append("totalFailCount: ").append(totalFailCount).append("; ");
        sb.append("samplingTps: ").append(samplingTps).append("; ");
        sb.append("samplingLatency: ").append(samplingLatency).append("; ");
        sb.append("samplingFailCount: ").append(samplingFailCount).append("; ");
        return sb.toString();
    }

    public static void main(String[] args) {
        test();
    }

    private static void test() {
        ThreadStat threadStat = new ThreadStat();
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
