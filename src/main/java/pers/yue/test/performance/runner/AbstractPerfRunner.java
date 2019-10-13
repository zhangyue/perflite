package pers.yue.test.performance.runner;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.exceptions.runtime.TestRunException;
import pers.yue.stoppable.Stoppable;
import pers.yue.test.performance.config.PerfConfig;
import pers.yue.test.performance.datastore.DataStore;
import pers.yue.test.performance.launcher.Launcher;
import pers.yue.test.performance.report.Reporter;
import pers.yue.test.performance.stat.GlobalStat;
import pers.yue.test.performance.stat.RequestResult;
import pers.yue.test.performance.stat.StatInfo;
import pers.yue.test.performance.stat.ThreadStat;
import pers.yue.common.util.LogUtil;
import pers.yue.common.util.ThreadUtil;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static pers.yue.test.performance.config.PerfConfig.*;

/**
 * Perf runner core class.
 *
 * Created by zhangyue182 on 2018/09/17
 */
public abstract class AbstractPerfRunner <DATA_INFO> implements Runnable, PerfRunner, Stoppable {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private int logLevel = LogUtil.LOG_LEVEL_VV_FUNCTIONAL;

    private PerfConfig<DATA_INFO> config;

    private long timeoutSecond = DEFAULT_TIMEOUT_SECOND;
    private int maxFailRate = DEFAULT_MAX_FAIL_RATE;
    private int maxTps = DEFAULT_MAX_TPS;
    private int statInterval = DEFAULT_STAT_INTERVAL;
    private boolean countTopPercentile = DEFAULT_COUNT_TP;
    private int numErrorsToCalmDown = DEFAULT_NUM_ERRORS_TO_CALM_DOWN;
    private long calmDownMs = DEFAULT_CALM_DOWN_MS;
    private static ThreadLocal<Integer> numContinuousErrors = ThreadLocal.withInitial(() -> 0);

    protected int numLoop;
    private List<String> keys = null;
    protected DataStore<String, DATA_INFO> dataStore;

    private final long testStartMillis = System.currentTimeMillis();
    private long lastStatMillis = testStartMillis;
    private long lastIsEndOfTestCheck = testStartMillis;
    private long lastRequestMillis = 0L;

    private int threadCounter = 0;
    protected static final ThreadLocal<Integer> threadId = ThreadLocal.withInitial(() -> -1);
    private Map<Integer, ThreadStat> threadStats;
    private int sameMillisDifferentiator = 0;

    private Iterator[] threadIterators;
    private static ThreadLocal<Iterator> threadIterator = new ThreadLocal<>();

    private ReadWriteLock statLock = new ReentrantReadWriteLock();
    protected ReadWriteLock dataStoreLocker = new ReentrantReadWriteLock();

    private static Reporter reporter = null;

    private StatInfo finalStatInfo;

    private RateLimiter rateLimiter;

    private boolean isNotifiedStop = false;

    protected AbstractPerfRunner(PerfConfig<DATA_INFO> config) {
        init(config);
    }

    private void init(PerfConfig<DATA_INFO> config) {
        this.config = config;
        int numThread = config.getNumThread();
        this.timeoutSecond = config.getTimeoutSecond();
        this.maxFailRate = config.getMaxFailRate();
        this.maxTps = config.getMaxTps();
        this.statInterval = config.getStatInterval();
        this.countTopPercentile = config.isCountTopPercentile();
        this.numErrorsToCalmDown = config.getNumErrorsToCalmDown();
        this.calmDownMs = config.getCalmDownMs();
        this.numLoop = config.getNumLoop();
        this.dataStore = config.getDataStore();

        List<String> keys;
        String source = "data store";
        if(config.getKeysAccessMode() == KeysAccessMode.keyList) {
            LogUtil.logAndSOutInfo(logger, "With KeysAccessMode.keyList");
            keys = config.getKeys();
            source = "key list";
        } else if(config.getDataStore() != null){
            LogUtil.logAndSOutInfo(logger, "With KeysAccessMode.dataStore");
            keys = new ArrayList<>(config.getDataStore().getStore().keySet());
        } else {
            LogUtil.logAndSOutInfo(logger, "With KeysAccessMode.dataStore but dataStore is null");
            keys = new ArrayList<>();
        }
        if(keys.size() <= 0 && this instanceof IteratorRunner) {
            LogUtil.logAndSOutWarn(logger, keys.size() + " keys read from " + source);
        } else {
            LogUtil.logAndSOutInfo(logger, keys.size() + " keys read from " + source);
        }

        if(config.getFilter() != null) {
            keys = config.getFilter().filterKeys();
            if(keys.size() <= 0 && this instanceof IteratorRunner) {
                LogUtil.logAndSOutWarn(logger, keys.size() + " keys filtered");
            } else {
                LogUtil.logAndSOutInfo(logger, keys.size() + " keys filtered");
            }
        }

        this.keys = keys;
        threadIterators = multipleIterators(this.keys, numThread);

        threadStats = new ConcurrentHashMap<>();

        if(maxTps > 0) {
            rateLimiter = RateLimiter.create(maxTps);
        }
    }

    private static Iterator<String>[] multipleIterators(List<String> keys, int numIterators) {
        Iterator<String>[] iterators = new Iterator[numIterators];
        int subListLength = keys.size() / iterators.length;
        int remainder = keys.size() % iterators.length;
        int offset = 0;

        for(int i = 0; i < iterators.length; i++) {
            int subListFromIndex = subListLength * i + offset;
            int subListToIndex = subListFromIndex + subListLength;
            if(remainder > 0) {
                subListToIndex++;
                remainder--;
                offset++;
            }
            List<String> subList = keys.subList(subListFromIndex, subListToIndex <= keys.size() ? subListToIndex : keys.size());
            List<String> threadList = new ArrayList<>(Arrays.asList(new String[subList.size()]));
            Collections.copy(threadList, subList);
            iterators[i] = threadList.iterator();
        }
        return iterators;
    }

    // For testing multipleIterators()
    public static void main(String[] args) {
        String[] keys = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
        Iterator<String>[] iterators = multipleIterators(Arrays.asList(keys), 5);
        for(Iterator it : iterators) {
            System.out.println("foo");
            while(it.hasNext()) {
                System.out.println((String)it.next());
            }
        }
    }

    protected void checkNumLoop() {
        if(numLoop <= 0) {
            logger.error("Invalid numLoop: {}", numLoop);
            throw new TestRunException("Invalid numLoop: " + numLoop);
        }
    }

    public PerfConfig<DATA_INFO> getConfig() {
        return config;
    }

    public void setLogLevel(int logLevel) {
        this.logLevel = logLevel;
    }

    public int getLogLevel() {
        return logLevel;
    }

    private void setThreadId() {
        threadId.set(threadCounter++);
    }

    public List<String> getKeys() {
        return keys;
    }

    public abstract RequestResult doAction() throws UnsupportedEncodingException, NoSuchAlgorithmException;

    public abstract RequestResult doAction(String key) throws UnsupportedEncodingException, NoSuchAlgorithmException;

    private RequestResult runOnce()  throws UnsupportedEncodingException, NoSuchAlgorithmException {
        RequestResult result = doAction();
        calmDownOrNot(result);
        return result;
    }

    private RequestResult runOnce(String key)  throws UnsupportedEncodingException, NoSuchAlgorithmException {
        RequestResult result = doAction(key);
        calmDownOrNot(result);
        return result;
    }

    private boolean isCalmDown() {
        return numContinuousErrors.get() == numErrorsToCalmDown;
    }

    private void countContinuousError() {
        numContinuousErrors.set(numContinuousErrors.get() + 1);
    }

    private void clearContinuousError() {
        numContinuousErrors.set(0);
    }

    private void calmDownOrNot(RequestResult result) {
        if(result.isSuccess()) {
            clearContinuousError();
        } else {
            if(isCalmDown()) {
                clearContinuousError();
                ThreadUtil.sleep(calmDownMs, TimeUnit.MILLISECONDS);
            } else {
                countContinuousError();
            }
        }
    }

    public void run() {
        synchronized (this) {
            setThreadId();
            threadStats.put(threadId.get(), new ThreadStat(threadId.get(), logLevel, countTopPercentile));
        }

        preRun();

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
                Launcher.recordFinalStatInfo(finalStatInfo);
            }

            postRun();
        }
    }

    protected abstract void preRun();
    protected abstract void postRun();

    /*
    For PUT
     */
    private void runWithLoop() {
        for (int i = 0; i < numLoop; i++) {
            acquireRateLimiter();

            if(logLevel >= LogUtil.LOG_LEVEL_V_LIGHT_STRESS) {
                logger.info("Thread {} loop {}.", Thread.currentThread().getName(), i);
            }

            try {
                doTreadStat(runOnce());
            } catch (Throwable e) {
                logger.error("Run failed with {}.", e.getClass().getSimpleName(), e);
                doTreadStat(new RequestResult(0,0,false));
                if(threadId.get() == 0) {
                    ThreadUtil.sleep(statInterval + 1); // To make sure global stat runs.
                }
            }

            if(isEndOfTest()) {
                doGlobalStatAnyWay();
                break;
            }

            doGlobalStat();
        }
    }

    /*
    For GET
     */
    private void runWithKeysAndLoop() {
        for (int i = 0; i < numLoop; i++) {
            acquireRateLimiter();

            if(logLevel >= LogUtil.LOG_LEVEL_V_LIGHT_STRESS) {
                logger.info("Thread {} loop {}.", Thread.currentThread().getName(), i);
            }

            if(keys == null || keys.size() == 0) {
                doTreadStat(new RequestResult(System.currentTimeMillis(), 0, false));
                break;
            }

            try {
                String key = keys.get(new Random().nextInt(keys.size()));
                doTreadStat(runOnce(key));
            } catch (Throwable e) {
                logger.error("Run failed with {}.", e.getClass().getSimpleName(), e);
                doTreadStat(new RequestResult(0,0,false));
                if(threadId.get() == 0) {
                    ThreadUtil.sleep(statInterval + 1); // To make sure global stat runs.
                }
            }

            if(isEndOfTest()) {
                doGlobalStatAnyWay();
                break;
            }

            doGlobalStat();
        }
    }

    /*
    For GET all and DELETE
    This has lower concurrency performance because of the lock.
     */
    private void runWithIterator() {
        if(logger.isDebugEnabled()) {
            logger.info("threadIterator set with threadId {}", threadId.get());
        }
        threadIterator.set(threadIterators[threadId.get()]);

        String key;

        int i = 0;
        while (true) {
            if(numLoop > 0) {
                if(i == numLoop) {
                    break;
                }
            }

            acquireRateLimiter();

            if(logLevel >= LogUtil.LOG_LEVEL_V_LIGHT_STRESS) {
                logger.info("Thread {} loop {}.", Thread.currentThread().getName(), i);
            }

            if(!threadIterator.get().hasNext()){
                logger.info("No more keys.");
                break;
            }
            key = (String) threadIterator.get().next();
            try {
                threadIterator.get().remove();
            } catch (ConcurrentModificationException e) {
                logger.warn("{} when iterator.remove()", e.getClass().getSimpleName(), e);
            }

            try {
                doTreadStat(runOnce(key));
            } catch (Throwable e) {
                logger.error("Run failed with {}.", e.getClass().getSimpleName(), e);
                doTreadStat(new RequestResult(0,0,false));
                if(threadId.get() == 0) {
                    ThreadUtil.sleep(statInterval + 1); // To make sure global stat runs.
                }
            }

            if(isEndOfTest()) {
                doGlobalStatAnyWay();
                break;
            }

            doGlobalStat();

            i++;
        }
    }

    private void acquireRateLimiter() {
        if(maxTps > 0) {
            rateLimiter.acquire();
        }
    }

    private void doTreadStat(RequestResult requestResult) {
        try {
            statLock.readLock().lock();
            threadStats.get(threadId.get()).stat(requestResult);
        } finally {
            statLock.readLock().unlock();
        }
    }

    private void doGlobalStat() {
        if(threadId.get() > 0) {
            return; // Only thread 0 does global stat.
        }

        long now = System.currentTimeMillis();

        if((now - lastStatMillis) / 1000 < statInterval) {
            return; // Not time yet.
        }

        doGlobalStatAnyWay();
    }

    private void doGlobalStatAnyWay() {
        try {
            statLock.writeLock().lock();

            StatInfo statInfo = new GlobalStat(logLevel, threadStats, testStartMillis, lastStatMillis).stat();
            if (statInfo == null) {
                return;
            }

            lastStatMillis = statInfo.getStatTimeMillis();

            statInfo.dump(reporter);

            finalStatInfo = statInfo;
        } finally {
            statLock.writeLock().unlock();
        }
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
     * Record the test data description to data store, for upcoming data verification throw GET.
     * @param key The key of the data.
     * @param dataInfo The DataInfo object of the data.
     */
    protected void addToDataStore(String key, DATA_INFO dataInfo) {
        if(dataStore == null) {
            return;
        }

        try {
            dataStoreLocker.writeLock().lock();
            dataStore.getStore().put(key, dataInfo);
            keys.add(key);
        } finally {
            dataStoreLocker.writeLock().unlock();
        }
    }

    /**
     * Remove the test data description from data store.
     * @param key The key of the data.
     */
    protected void removeFromDataStore(String key) {
        if(dataStore == null) {
            return;
        }

        try {
            dataStoreLocker.writeLock().lock();
            dataStore.getStore().remove(key);
            keys.remove(key);
        } finally {
            dataStoreLocker.writeLock().unlock();
        }
    }

    protected String getRandomKeyFromDataStore() {
        if(dataStore == null) {
            ThreadUtil.throwRuntimeException("Data store is null.");
            return "dummy";
        }

        try {
            dataStoreLocker.readLock().lock();
            return keys.get(new Random().nextInt(keys.size()));
        } finally {
            dataStoreLocker.readLock().unlock();
        }
    }

    protected String getRandomKeyForDeleteFromDataStore() {
        if(dataStore == null) {
            ThreadUtil.throwRuntimeException("Data store is null.");
            return "dummy";
        }

        try {
            dataStoreLocker.writeLock().lock();
            return keys.get(new Random().nextInt(keys.size()));
        } finally {
            dataStoreLocker.writeLock().unlock();
        }
    }

    private boolean isEndOfTest() {
        if(isNotifiedStop) {
            if(threadId.get() == 0) { // Only the first thread print this message.
                ThreadUtil.sleep(5); // To make sure the log comes after other thread's normal operations.
                LogUtil.logAndSOutInfo(logger, "Test stopped by notification.");
            }
            return true;
        }

        if(timeoutSecond > 0 && System.currentTimeMillis() > testStartMillis + timeoutSecond * 1000) {
            if(threadId.get() == 0) { // Only the first thread print this message.
                ThreadUtil.sleep(5); // To make sure the log comes after other thread's normal operations.
                LogUtil.logAndSOutInfo(logger, "Test stopped by timeout - " + timeoutSecond + " seconds.");
            }
            return true;
        }

        if((System.currentTimeMillis() - lastIsEndOfTestCheck) / 1000 < statInterval) {
            return false; // Check maxFailRate only every statInterval to reduce performance impact.
        }
        lastIsEndOfTestCheck = System.currentTimeMillis();

        try {
            statLock.readLock().lock();
            if (finalStatInfo == null) {
                return false;
            }

            int failRate = finalStatInfo.getNumFailedRequests() * 100 / finalStatInfo.getNumRequests();
            if (failRate > maxFailRate) {
                LogUtil.logAndSOutInfo(logger, "Reach maxFailRate: " + maxFailRate + " - Current: " + failRate + "%");
                if (threadId.get() == 0) {
                    ThreadUtil.sleep(statInterval * 2);
                    LogUtil.logAndSOutInfo(logger, "Test stopped by failure threshold reached - " + maxFailRate + "%.");
                }
                return true;
            }
            return false;
        } finally {
            statLock.readLock().unlock();
        }
    }

    /**
     * Generate key based on time stamp but unique even two keys are generated in same millisecond.
     * @param prefix Key prefix
     * @return A unique key based on the key prefix.
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

        return prefix + "-" + timeStamp + "-" + String.format("%04d", sameMillisDifferentiator);
    }

    protected void assertKeysNotNull() {
        if(keys == null || keys.isEmpty()) {
            logger.error("keys is null or empty");
            throw new TestRunException("keys is null or empty");
        }
    }

    @Override
    public synchronized void notifyStop() {
        LogUtil.logAndSOutInfo(logger, "Notify stop.");
        isNotifiedStop = true;
    }
}
