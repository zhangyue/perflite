package pers.yue.performance.runner;

import pers.yue.performance.Launcher;
import pers.yue.performance.report.Reporter;
import ffzy.performance.stat.*;
import pers.yue.performance.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.performance.stat.GlobalStat;
import pers.yue.performance.stat.RequestResult;
import pers.yue.performance.stat.StatInfo;
import pers.yue.performance.stat.ThreadStat;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by zhangyue58 on 2018/09/17
 */
public abstract class AbstractPerfRunner implements Runnable, PerfRunner {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    protected int numLoop;
    protected List<String> keys = null;
    protected Map<String, String> objectStore = new TreeMap<>();

    public static final int DEFAULT_STAT_INTERVAL = 3; // In seconds

    private static int statInterval = DEFAULT_STAT_INTERVAL;

    private final long testStartMillis = System.currentTimeMillis();

    private long lastStatMillis = testStartMillis;
    private long lastRequestMillis = 0L;

    private int threadCounter = 0;
    private static final ThreadLocal<Integer> threadId = ThreadLocal.withInitial(() -> -1);
    private Map<Integer, ThreadStat> threadStats;
    private int sameMillisDifferentiator = 0;

    private Iterator iterator;

    protected Lock threadIdLocker = new ReentrantLock();
    protected Lock objectStoreLocker = new ReentrantLock();
    protected Lock iteratorLocker = new ReentrantLock();

    private static Reporter reporter = null;

    protected static final ThreadLocal<StatInfo> finalStatInfo = ThreadLocal.withInitial(() -> null);

    /**
     * For put, get and head object
     *
     * @param numLoop     For put object: numLoop must be greater than 0.
     *                    For get and head object:
     *                    If numLoop is greater than 0, then randomly select objects in numLoop.
     *                    If numLoop == -1, then get and verify all objects, or head all objects.
     * @param objectStore Stores key list and their MD5
     */
    public AbstractPerfRunner(int numLoop, Map<String, String> objectStore) {
        init(numLoop, null, objectStore);
    }

    /**
     * For get without data verification and head object.
     */

    /**
     * For get without data verification and head object.
     *
     * @param numLoop For get and head object:
     *                If numLoop is greater than 0, then randomly select objects in numLoop.
     *                If numLoop == -1, then get or head all objects.
     * @param keys    Key list
     */
    public AbstractPerfRunner(int numLoop, List<String> keys) {
        init(numLoop, keys, null);
    }

    /**
     * For put, get and delete object in a single loop
     *
     * @param numLoop Must be greater than 0.
     */
    public AbstractPerfRunner(int numLoop) {
        init(numLoop, null, objectStore);
    }

    /**
     * For delete object (delete them all)
     *
     * @param keys Key list
     */
    public AbstractPerfRunner(List<String> keys) {
        init(-1, keys, null);
    }

    protected void init(int numLoop, List<String> keys, Map<String, String> objectStore) {

        this.numLoop = numLoop;
        this.keys = keys;
        this.objectStore = objectStore;

        if (keys == null || keys.size() == 0) {
            if (objectStore != null && objectStore.size() > 0) {
                this.keys = new ArrayList<>(objectStore.keySet());
            }
        }

        if (keys != null) {
            iterator = keys.iterator();
        } else if (objectStore != null) {
            iterator = objectStore.keySet().iterator();
        }

        threadStats = new ConcurrentHashMap<>();
    }

    private void setThreadId() {
        threadId.set(threadCounter++);
    }

    public abstract RequestResult doAction(int count) throws UnsupportedEncodingException, NoSuchAlgorithmException;

    public abstract RequestResult doAction(String key) throws UnsupportedEncodingException, NoSuchAlgorithmException;

    public void run() {
        try {
            threadIdLocker.lock();
            setThreadId();
        } finally {
            threadIdLocker.unlock();
        }

        try {
            if(this instanceof IteratorRunner && this instanceof LoopRunner) {
                if (numLoop > 0) {
                    runWithKeysAndLoop();
                } else {
                    runWithIterator();
                }
            } else if(this instanceof LoopRunner) {
                runWithLoop();
            } else if(this instanceof IteratorRunner) {
                runWithIterator();
            } else {
                logger.error("Unsupported runner: {}", this.getClass().getSimpleName());
            }

        } finally {
            if(threadId.get() == 0) {
                Launcher.recordFinalStatInfo(finalStatInfo.get());
            }
        }
    }

    /*
    For PUT
     */
    private void runWithLoop() {
        StatInfo statInfo;
        for (int i = 0; i < numLoop; i++) {
            logger.info("Thread {} loop {}.", Thread.currentThread().getName(), i);

            try {
                doTreadStat(doAction(i));
                statInfo = doGlobalStat();
                if(statInfo != null) {
                    finalStatInfo.set(statInfo);
                }

            } catch (Exception e) {
                logger.error("Request failed.", e);
            }
        }
    }

    /*
    For GET
     */
    private void runWithKeysAndLoop() {
        if (null == keys) {
            return;
        }

        StatInfo statInfo;
        for (int i = 0; i < numLoop; i++) {
            logger.info("Thread {} loop {}.", Thread.currentThread().getName(), i);

            try {
                String key = keys.get(new Random().nextInt(keys.size()));
                doTreadStat(doAction(key));
                statInfo = doGlobalStat();
                if(statInfo != null) {
                    finalStatInfo.set(statInfo);
                }

            } catch (Exception e) {
                logger.error("Request failed.", e);
            }
        }
    }

    /*
    For GET all and DELETE
    This has lower concurrency performance because of the lock.
     */
    private void runWithIterator() {
        if (null == keys) {
            return;
        }

        StatInfo statInfo;
        String key;
        int i = 0;
        while (true) {
            if(numLoop > 0) {
                if(i == numLoop) {
                    break;
                }
            }

            logger.info("Thread {} loop {}.", Thread.currentThread().getName(), i);
            try {
                iteratorLocker.lock();
                if (!iterator.hasNext()) {
                    break;
                }
                key = (String) iterator.next();
                iterator.remove();
            } finally {
                iteratorLocker.unlock();
            }

            try {
                doTreadStat(doAction(key));
                statInfo = doGlobalStat();
                if(statInfo != null) {
                    finalStatInfo.set(statInfo);
                }
            } catch (Exception e) {
                logger.error("{} when delete object.", e.getClass().getSimpleName(), e);
            }

            i++;
        }
    }

    private void doTreadStat(RequestResult requestResult) {
        int tid = threadId.get();
        if(threadStats.get(tid) == null) {
            threadStats.put(tid, new ThreadStat());
        }

        threadStats.get(tid).stat(requestResult);
    }

    private StatInfo doGlobalStat() {
        if(threadId.get() > 0) {
            return null; // Only thread 0 does global stat.
        }

        long now = System.currentTimeMillis();

        if((now - lastStatMillis) / 1000 < statInterval) {
            return null; // Not time yet.
        }

        StatInfo statInfo = new GlobalStat(threadStats, testStartMillis, lastStatMillis).stat();
        if(statInfo == null) {
            return null;
        }

        lastStatMillis = statInfo.getStatTimeMillis();

        statInfo.dump(reporter);

        return statInfo;
    }

    public void setStatInterval(int statInterval) {
        this.statInterval = statInterval;
    }

    public void setReporter(Reporter reporter) {
        AbstractPerfRunner.reporter = reporter;
    }

    public int getNumLoop() {
        return numLoop;
    }

    /**
     * Generate key based on time stamp but unique even two keys are generated in same millisecond.
     * @param prefix
     * @return
     */
    protected synchronized String generateUniqueKey(String prefix) {
        long timeStamp = System.currentTimeMillis();

        if(timeStamp <= lastRequestMillis) {
            timeStamp = lastRequestMillis;
            sameMillisDifferentiator++;
        } else {
            lastRequestMillis = timeStamp;
            sameMillisDifferentiator = 0;
        }

        return new StringBuffer(prefix).append("-").append(timeStamp).append("-").append(String.format("%04d", sameMillisDifferentiator)).toString();
    }
}
