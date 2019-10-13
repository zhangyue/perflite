package pers.yue.test.performance.launcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.test.performance.report.Reporter;
import pers.yue.test.performance.runner.PerfRunner;
import pers.yue.test.performance.stat.StatInfo;
import pers.yue.util.DateUtil;
import pers.yue.util.MathUtil;
import pers.yue.util.ThreadUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Launches the test runners in a concurrent manner.
 *
 * Created by zhangyue182 on 2017/11/29
 */
public class Launcher {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private Runnable objectRunner;

    private int numLoop;
    private int numThread;
    private Reporter reporter;

    private List<Thread> workerThreads = Collections.synchronizedList(new ArrayList<>());
    private static StatInfo finalStatInfo;

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
    }

    public void launch() {
        logger.info("numLoop: {}", numLoop);
        logger.info("numThread: {}", numThread);

        for(int i = 0; i < numThread; i++) {
            Thread t = new Thread(objectRunner);
            t.start();
            workerThreads.add(t);
//            ThreadUtil.sleep(1, TimeUnit.MILLISECONDS);
        }

        long start = System.currentTimeMillis();

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
            if(finalStatInfo.getNumFailedRequests() > 0) {
                logger.info("Fail request #: {}", (finalStatInfo.getNumFailedRequests()));
            }
            logger.info("Fail rate (%): {}", ((float)finalStatInfo.getNumFailedRequests() / finalStatInfo.getNumRequests() * 100));
            if(finalStatInfo.getTp99() > 0) {
                logger.info("TP90: {}", finalStatInfo.getTp90());
                logger.info("TP95: {}", finalStatInfo.getTp95());
                logger.info("TP99: {}", finalStatInfo.getTp99());
            }
        }

        if(reporter != null) {
            // To tell the reader that the test is done.
            reporter.update(System.currentTimeMillis(), 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0);
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

    public static void recordFinalStatInfo(StatInfo statInfo) {
        finalStatInfo = statInfo;
    }

    public static StatInfo getFinalStatInfo() {
        return finalStatInfo;
    }
}
