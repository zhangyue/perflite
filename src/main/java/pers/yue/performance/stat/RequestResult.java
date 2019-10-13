package pers.yue.performance.stat;

/**
 * Created by zhangyue58 on 2018/09/13
 */
public class RequestResult {
    private long startTime;
    private long latency;
    private boolean success = true;

    public RequestResult(long startTime, long latency) {
        this.startTime = startTime;
        this.latency = latency;
    }

    public RequestResult(long startTime, long latency, boolean success) {
        this.startTime = startTime;
        this.latency = latency;
        this.success = success;
    }

    public long getStartTime() {
        return startTime;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public long getLatency() {
        return latency;
    }
}
