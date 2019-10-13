package pers.yue.test.performance.config;

import pers.yue.test.performance.datastore.DataStore;

import java.util.ArrayList;
import java.util.List;

/**
 * A config class that holds all the common properties required by performance test.
 *
 * Created by Zhang Yue on 3/31/2019
 */
public class PerfConfig <DATA_INFO> {
    public static final int DEFAULT_NUM_THREAD = 1;
    public static final int DEFAULT_TIMEOUT_SECOND = 3600 * 12;
    public static final int DEFAULT_MAX_FAIL_RATE = 80;
    public static final int DEFAULT_MAX_TPS = -1;
    public static final int DEFAULT_NUM_ERRORS_TO_CALM_DOWN = 5;
    public static final long DEFAULT_CALM_DOWN_MS = 500;
    public static final int DEFAULT_STAT_INTERVAL = 3;
    public static final boolean DEFAULT_COUNT_TP = true;
    public static final int DEFAULT_NUM_LOOP = Integer.MAX_VALUE;

    /**
     * Number of test threads.
     */
    private int numThread = DEFAULT_NUM_THREAD;

    /**
     * Timeout (in seconds) of the test. When it's reached, the test will be stopped even if numLoop is not reached.
     */
    private int timeoutSecond = DEFAULT_TIMEOUT_SECOND;

    /**
     * Stop the test if more than this rate of requests fail.
     */
    private int maxFailRate = DEFAULT_MAX_FAIL_RATE;

    /**
     * Maximum TPS of the test, restrained in test client side.
     */
    private int maxTps = DEFAULT_MAX_TPS;

    /**
     * Calm down on this amount of continuous errors, to avoid log being stuffed by repeating error and stack trace.
     */
    private int numErrorsToCalmDown = DEFAULT_NUM_ERRORS_TO_CALM_DOWN;

    /**
     * Calm down on a number of continuous errors (defined by errorNumToCalmDown) and sleep this period of time,
     * to avoid log being stuffed by repeating error and stack trace.
     */
    private long calmDownMs = DEFAULT_CALM_DOWN_MS;

    /**
     * Stat interval (in seconds).
     */
    private int statInterval = DEFAULT_STAT_INTERVAL;

    /**
     * Whether count top percentile of the requests.
     */
    private boolean countTopPercentile = DEFAULT_COUNT_TP;

    /**
     * Number of loop each thread runs.
     */
    private int numLoop = DEFAULT_NUM_LOOP;

    /**
     * The DB that stores attributes of the test data sent to SUT.
     */
    private DataStore<String, DATA_INFO> dataStore = null;

    /**
     * The list of keys read from data store.
     */
    private List<String> keys = null;

    /**
     * Filters out some keys and only keep a subset of the keys to test with.
     */
    private Filter filter = null;

    /**
     * keyList: Only retrieves keys from keyList instead of data store (so no chance to verify with the attributes stored in data store).
     * dataStore: Retrieves key list along with data info from data store, for data verification.
     */
    public enum KeysAccessMode {
        keyList, dataStore
    }

    private KeysAccessMode keysAccessMode = KeysAccessMode.dataStore;

    public PerfConfig() {

    }

    /**
     * For put, get and head object
     *
     * @param timeoutSecond Duration of the test, even numLoop is not met.
     * @param maxFailRate Stop the test if failure ratio reaches this threshold.
     * @param maxTps Maximum TPS.
     * @param statInterval Interval between each stat, a.k.a sampling time window length.
     * @param countTopPercentile Whether count top percentile of the requests.
     * @param numLoop     For put object: numLoop must be greater than 0.
     *                    For get and head object:
     *                    If numLoop is greater than 0, then randomly select objects in numLoop.
     *                    If numLoop == -1, then get and verify all objects, or head all objects.
     * @param dataStore Data store contains data description map.
     */
    public PerfConfig(int numThread, Integer timeoutSecond, Integer maxFailRate, int maxTps, Integer statInterval,
                      boolean countTopPercentile, Integer numLoop, DataStore<String, DATA_INFO> dataStore) {
        initCommonParameters(numThread, timeoutSecond, maxFailRate, maxTps, statInterval, countTopPercentile, numLoop);
        this.dataStore = dataStore;
    }

    /**
     * For get without data verification and head object.
     *
     * @param timeoutSecond Duration of the test, even numLoop is not met.
     * @param maxFailRate Stop the test if failure ratio reaches this threshold.
     * @param maxTps Maximum TPS.
     * @param statInterval Interval between each stat, a.k.a sampling time window length.
     * @param countTopPercentile Whether count top percentile of the requests.
     * @param numLoop For get and head object:
     *                If numLoop is greater than 0, then randomly select objects in numLoop.
     *                If numLoop == -1, then get or head all objects.
     * @param keys    Key list
     */
    public PerfConfig(int numThread, Integer timeoutSecond, Integer maxFailRate, int maxTps, Integer statInterval,
                      boolean countTopPercentile, Integer numLoop, List<String> keys) {
        initCommonParameters(numThread, timeoutSecond, maxFailRate, maxTps, statInterval, countTopPercentile, numLoop);
        this.keys = keys;
        keysAccessMode = KeysAccessMode.keyList;
    }

    /**
     * For put, get and delete object in a single loop
     *
     * @param timeoutSecond Duration of the test, even numLoop is not met.
     * @param maxFailRate Stop the test if failure ratio reaches this threshold.
     * @param maxTps Maximum TPS.
     * @param statInterval Interval between each stat, a.k.a sampling time window length.
     * @param countTopPercentile Whether count top percentile of the requests.
     * @param numLoop Must be greater than 0.
     */
    public PerfConfig(int numThread, Integer timeoutSecond, Integer maxFailRate, int maxTps, Integer statInterval,
                      boolean countTopPercentile, Integer numLoop) {
        initCommonParameters(numThread, timeoutSecond, maxFailRate, maxTps, statInterval, countTopPercentile, numLoop);
    }

    private void initCommonParameters(int numThread, Integer timeoutSecond, Integer maxFailRate, int maxTps, Integer statInterval,
                                      boolean countTopPercentile, Integer numLoop) {
        this.numThread = numThread;
        if(timeoutSecond != null) {
            this.timeoutSecond = timeoutSecond;
        }
        if(maxFailRate != null) {
            this.maxFailRate = maxFailRate;
        }
        if(statInterval != null) {
            if(statInterval > 0) {
                this.statInterval = statInterval;
            } else {
                this.statInterval = DEFAULT_STAT_INTERVAL;
            }
        }
        this.countTopPercentile = countTopPercentile;
        this.maxTps = maxTps;
        if(numLoop != null) {
            this.numLoop = numLoop;
        }
    }

    public PerfConfig<DATA_INFO> withNumThread(int numThread) {
        this.numThread = numThread;
        return this;
    }

    public PerfConfig<DATA_INFO> withTimeout(int timeoutSecond) {
        this.timeoutSecond = timeoutSecond;
        return this;
    }

    public PerfConfig<DATA_INFO> withMaxFailRate(int maxFailRate) {
        this.maxFailRate = maxFailRate;
        return this;
    }

    public PerfConfig<DATA_INFO> withMaxTps(int maxTps) {
        this.maxTps = maxTps;
        return this;
    }

    public PerfConfig<DATA_INFO> withNumErrorsToCalmDown(int errorNumToCalmDown) {
        this.numErrorsToCalmDown = errorNumToCalmDown;
        return this;
    }

    public PerfConfig<DATA_INFO> withCalmDownMs(long calmDownMs) {
        this.calmDownMs = calmDownMs;
        return this;
    }

    public PerfConfig<DATA_INFO> withStatInterval(int statInterval) {
        this.statInterval = statInterval;
        return this;
    }

    public PerfConfig<DATA_INFO> withNumLoop(int numLoop) {
        this.numLoop = numLoop;
        return this;
    }

    public PerfConfig<DATA_INFO> withDataStore(DataStore<String, DATA_INFO> dataStore) {
        this.dataStore = dataStore;
        keysAccessMode = KeysAccessMode.dataStore;
        return this;
    }

    public PerfConfig<DATA_INFO> withDataStore() {
        keysAccessMode = KeysAccessMode.dataStore;
        return this;
    }

    public PerfConfig<DATA_INFO> withKeys(List<String> keys) {
        this.keys = keys;
        keysAccessMode = KeysAccessMode.keyList;
        return this;
    }

    public PerfConfig<DATA_INFO> withKeyList() {
        this.keys = new ArrayList<>(this.dataStore.getStore().keySet());
        keysAccessMode = KeysAccessMode.keyList;
        return this;
    }

    public PerfConfig<DATA_INFO> withFilter(Filter filter) {
        this.filter = filter;
        return this;
    }

    public KeysAccessMode getKeysAccessMode() {
        return keysAccessMode;
    }

    public int getNumThread() {
        return numThread;
    }

    public long getTimeoutSecond() {
        return timeoutSecond;
    }

    public int getMaxFailRate() {
        return maxFailRate;
    }

    public int getMaxTps() {
        return maxTps;
    }

    public int getNumErrorsToCalmDown() {
        return numErrorsToCalmDown;
    }

    public long getCalmDownMs() {
        return calmDownMs;
    }

    public int getStatInterval() {
        return statInterval;
    }

    public boolean isCountTopPercentile() {
        return countTopPercentile;
    }

    public int getNumLoop() {
        return numLoop;
    }

    public DataStore<String, DATA_INFO> getDataStore() {
        return dataStore;
    }

    public List<String> getKeys() {
        return keys;
    }

    public Filter getFilter() {
        return this.filter;
    }
}
