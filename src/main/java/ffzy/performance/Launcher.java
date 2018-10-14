package ffzy.performance;

import ffzy.performance.report.Reporter;
import ffzy.performance.runner.PerfRunner;
import ffzy.performance.stat.StatInfo;
import ffzy.performance.util.DateUtil;
import ffzy.performance.util.MathUtil;
import ffzy.performance.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by zhangyue58 on 2017/11/29
 */
public class Launcher {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private Runnable objectRunner;

    private int numLoop;
    private int numThread;
    private int timeoutInSecond;
    private int threadStatusCheckIntervalInSecond = 5;
    private Reporter reporter;

    private List<Thread> workerThreads = Collections.synchronizedList(new ArrayList<>());
    private static List<Double> singleThreadMeanTimes;
    private static StatInfo finalStatInfo;

    public Launcher(PerfRunner runner, int numThread, int threadStatusCheckIntervalInSecond) {
        init(runner, numThread, reporter);
        this.threadStatusCheckIntervalInSecond = threadStatusCheckIntervalInSecond;
    }

    public Launcher(PerfRunner runner, int numThread) {
        init(runner, numThread, null);
    }

    public Launcher(PerfRunner runner, int numThread, Reporter reporter) {
        init(runner, numThread, reporter);
    }

    private void init(PerfRunner runner, int numThread, Reporter reporter) {
        this.objectRunner = runner;
        this.numLoop = runner.getNumLoop();
        this.numThread = numThread;
        this.reporter = reporter;
        timeoutInSecond = numLoop * numThread * 5;
        singleThreadMeanTimes = new ArrayList<>();
    }

    public void launch() {
        logger.info("numLoop: {}", numLoop);
        logger.info("numThread: {}", numThread);
        logger.info("timeOutInSecond: {}", timeoutInSecond);

        for(int i = 0; i < numThread; i++) {
            Thread t = new Thread(objectRunner);
            t.start();
            workerThreads.add(t);
        }

        long start = System.currentTimeMillis();

//        waitForAllThreads();
        waitForAllThreadsByJoin();

        long end = System.currentTimeMillis();
        double secondsUsed = (end - start) / 1000D;

        logger.info("numLoop:       {}", numLoop);
        logger.info("numThread:     {}", numThread);
        logger.info("Start:         {} / {}", DateUtil.formatTime(start, DateUtil.FORMAT_Z), start);
        logger.info("End:           {} / {}", DateUtil.formatTime(end, DateUtil.FORMAT_Z), end);
        logger.info("Time used (s): {}", secondsUsed);
        if(finalStatInfo != null) {
            logger.info("Mean latency:  {}", MathUtil.round(finalStatInfo.getLatency(), 2));
            logger.info("TPS:           {}", MathUtil.round(finalStatInfo.getTps(), 2));
            logger.info("Fail rate (%): {}", (finalStatInfo.getNumFailedRequests() / finalStatInfo.getNumRequests() * 100));
        }

        if(reporter != null) {
            // To tell the reader that the test is done.
            reporter.update(System.currentTimeMillis(), 0, 0, 0,
                    0, 0, 0, 0, 0);
        }
    }

    private void waitForAllThreadsByJoin() {
        for(Thread t : workerThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error("Interrupted.", e);
            }
        }
    }

    // Need debug here but somehow we can just go with waitForAllThreadsByJoin().
    private void waitForAllThreads() {
        long start = System.currentTimeMillis();
        long timeOutInMillis = timeoutInSecond * 1000;

        String exitMessage = "Timed out.";
        while(!isTimedOut(start, timeOutInMillis)) {
            boolean hasAlive = false;
            for(Thread t : workerThreads) {
                if(t.isAlive()) {
                    hasAlive = true;
                }
            }
            if(hasAlive) {
                logger.info("Still has alive thread. Sleep {} seconds.", threadStatusCheckIntervalInSecond);
                ThreadUtil.sleep(threadStatusCheckIntervalInSecond);
            } else {
                exitMessage = "All threads done.";
                break;
            }
        }

        logger.info(exitMessage);
        logger.info("Global mean time: {}", getGlobalMeantime());
    }

    private boolean isTimedOut(long start, long timeOutInMillis) {
        long current = System.currentTimeMillis();

        if(current > start + timeOutInMillis) {
            return true;
        } else {
            return false;
        }
    }

    public static void recordFinalStatInfo(StatInfo statInfo) {
        finalStatInfo = statInfo;
    }

    private long getGlobalMeantime() {
        finalStatInfo.getLatency();
        double meanTimeInMillis = 0;

        if(Launcher.singleThreadMeanTimes == null) {
            logger.warn("Launcher.singleThreadMeanTimes is null!");
            return -1;
        }

        logger.debug("singleThreadMeanTimes.size(): {}", Launcher.singleThreadMeanTimes.size());

        if(Launcher.singleThreadMeanTimes.size() == 0) {
            logger.warn("Launcher.singleThreadMeanTimes.size() is 0!");
            return -1;
        }

        for(Double mt : Launcher.singleThreadMeanTimes) {
            logger.debug("mt: {}", mt);
            meanTimeInMillis += mt;
        }

        meanTimeInMillis /= Launcher.singleThreadMeanTimes.size();

        return (long)meanTimeInMillis;
    }
}
