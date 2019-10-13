package pers.yue.test.performance.report;

/**
 * The interface for reporting real-time test statistic metrics.
 *
 * Created by zhangyue182 on 2018/08/28
 */
public interface Reporter {
    void update(
            long date,
            long numRequests, float meanTps, float meanLatencyMillis,
            long cycleNumRequests, float cycleTps, float cycleLatencyMillis,
            int numFailedRequests, int cycleNumFailedRequests,
            long tp90, long tp95, long tp99
    );

    void printHeader();
    void repeatHeader();
}
