package ffzy.performance.report;

public interface Reporter {
    void update(
            long date,
            long numRequests, float meanTps, float meanLatencyMillis,
            long cycleNumRequests, float cycleTps, float cycleLatencyMillis,
            int numFailedRequests, int cycleNumFailedRequests
    );
}
