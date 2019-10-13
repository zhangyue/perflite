package pers.yue.test.performance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.test.performance.config.PerfConfig;
import pers.yue.test.performance.datastore.DataStore;
import pers.yue.test.performance.launcher.Launcher;
import pers.yue.test.performance.report.CsvReporter;
import pers.yue.test.performance.report.Reporter;
import pers.yue.test.performance.runner.PerfRunner;
import pers.yue.test.performance.stat.StatInfo;
import pers.yue.test.testcase.TestCase;
import pers.yue.common.util.LogUtil;
import pers.yue.common.util.PropertiesUtil;
import pers.yue.common.util.ThreadUtil;

import java.io.File;

import static pers.yue.common.util.PropertiesUtil.parseProperty;

/**
 * Performance test core class.
 *
 * Created by zhangyue182 on 2019/01/31
 */
public class PerfTestCore <DATA_INFO> {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private static final String DEFAULT_DATA_STORE_FILE_NAME = "data_store.db";
    private static final int DEFAULT_LOG_LEVEL = LogUtil.LOG_LEVEL_V_LIGHT_STRESS;
    private static final int DEFAULT_MIN_SUCCESS_RATE_FOR_PASS = 100;
    private static final String DEFAULT_PATH_TO_REPORTER_FILE = "report" + File.separator + "perf_report.csv";

    protected PerfConfig<DATA_INFO> config = new PerfConfig<>(); // Init with default config values.

    private DataStore<String, DATA_INFO> dataStore;
    private int logLevel = DEFAULT_LOG_LEVEL;
    private String dataStoreFileName = DEFAULT_DATA_STORE_FILE_NAME;
    private int minSuccessRate = DEFAULT_MIN_SUCCESS_RATE_FOR_PASS;
    private Reporter reporter;
    private String pathToReporterFile = DEFAULT_PATH_TO_REPORTER_FILE;

    public PerfTestCore() {
        start();
        loadProperties();
    }

    public StatInfo launchRunner(PerfRunner runner) {
        runner.setReporter(reporter);
        runner.setLogLevel(logLevel);

        Launcher launcher = new Launcher(runner, config.getNumThread(), reporter);
        launcher.launch();
        return Launcher.getFinalStatInfo();
    }

    private void start() {
        logger.info("[PERFORMANCE TEST START]");
    }

    private void end() {
        logger.info("[PERFORMANCE TEST END]\n");
    }

    public void assertResult(StatInfo resultStat) {
        try {
            if(resultStat == null) {
                logger.warn("No final result stat available. Possible the test is done too soon." +
                        " Try to enlarge numLoop to see the final stat.");
                return;
            }
            int numAll = resultStat.getNumRequests();
            int numSuccess = numAll - resultStat.getNumFailedRequests();
            TestCase.assertGE(numSuccess * 100 / numAll, minSuccessRate);

        } finally {
            end();
        }
    }

    public void loadProperties() {
        logger.info("Load properties from command line:");

        String numThreadProperty = System.getProperty(NUM_THREAD_PROPERTY_NAME);
        String timeoutProperty = System.getProperty(TIME_OUT_PROPERTY_NAME);
        String maxFailRateProperty = System.getProperty(MAX_FAIL_RATE_PROPERTY_NAME);
        String maxTpsProperty = System.getProperty(MAX_TPS_PROPERTY_NAME);
        String samplingIntervalProperty = System.getProperty(SAMPLING_INTERVAL_PROPERTY_NAME);
        String countTpProperty = System.getProperty((COUNT_TP_PROPERTY_NAME));
        String numLoopProperty = System.getProperty(NUM_LOOP_PROPERTY_NAME);
        String dbFileProperty = System.getProperty(DATA_STORE_FILE_PROPERTY_NAME);
        String logLevelProperty = System.getProperty(LOG_LEVEL_PROPERTY_NAME);
        String minSuccessRateProperty = System.getProperty((MIN_SUCCESS_RATE_FOR_PASS_PROPERTY_NAME));

        try {
            int numThread = parseProperty(config.getNumThread(), numThreadProperty);
            int timeoutSecond = (int) PropertiesUtil.parseTimeProperty(config.getTimeoutSecond(), timeoutProperty);
            int maxFailRate = (int)PropertiesUtil.parseTimeProperty(config.getMaxFailRate(), maxFailRateProperty);
            int maxTps = parseProperty(config.getMaxTps(), maxTpsProperty);
            int samplingInterval = parseProperty(config.getStatInterval(), samplingIntervalProperty);
            boolean countTp = parseProperty(config.isCountTopPercentile(), countTpProperty);
            int numLoop = parseProperty(config.getNumLoop(), numLoopProperty);
            dataStoreFileName = parseProperty(dataStoreFileName, dbFileProperty);
            logLevel = parseProperty(logLevel, logLevelProperty);
            minSuccessRate = parseProperty(minSuccessRate, minSuccessRateProperty);

            if(!dataStoreFileName.equals("null")) {
                dataStore = new DataStore<>(dataStoreFileName);
            } else {
                dataStore = null;
            }

            logger.info("======== numThread: {} ========", numThread);
            if(timeoutSecond > 0) {
                logger.info("======== timeoutSecond: {} ========", timeoutSecond);
            }
            logger.info("======== maxFailRate: {} ========", maxFailRate);
            logger.info("======== maxTps: {} ========", maxTps);
            logger.info("======== samplingInterval: {} ========", samplingInterval);
            if(countTp) {
                logger.info("======== countTp: {} ========", countTp);
            }
            logger.info("======== numLoop: {} ========", numLoop);
            logger.info("======== datastore: {} ========", dataStore == null? "null": dataStore.getStoreFile());

            config = new PerfConfig<>(numThread, timeoutSecond, maxFailRate, maxTps, samplingInterval, countTp, numLoop);
            config.withDataStore(dataStore);

            reporter = new CsvReporter(new File(pathToReporterFile));

        } catch (RuntimeException e) {
            logger.error("Exception when parsing system property.", e);
            throw e;
        }
    }

    public PerfConfig<DATA_INFO> getConfig() {
        return config;
    }

    public void setConfig(PerfConfig<DATA_INFO> config) {
        this.config = config;
    }

    public DataStore<String, DATA_INFO> getDataStore() {
        return dataStore;
    }

    public void setDataStore(DataStore<String, DATA_INFO> dataStore) {
        this.dataStore = dataStore;
    }

    public int getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(int logLevel) {
        this.logLevel = logLevel;
    }

    public int getMinSuccessRate() {
        return minSuccessRate;
    }

    public void setMinSuccessRate(int minSuccessRate) {
        this.minSuccessRate = minSuccessRate;
    }

    public PerfTestCore<DATA_INFO> withMinSuccessRate(int minSuccessRate) {
        this.minSuccessRate = minSuccessRate;
        return this;
    }

    public Reporter getReporter() {
        return reporter;
    }

    public void setReporter(Reporter reporter) {
        this.reporter = reporter;
    }

    public String getPathToReporterFile() {
        return pathToReporterFile;
    }

    public void setPathToReporterFile(String pathToReporterFile) {
        this.pathToReporterFile = pathToReporterFile;
    }

    private static final String NUM_THREAD_PROPERTY_NAME = "numThread";
    private static final String TIME_OUT_PROPERTY_NAME = "timeout";
    private static final String MAX_FAIL_RATE_PROPERTY_NAME = "maxFailRate";
    private static final String MAX_TPS_PROPERTY_NAME = "maxTps";
    private static final String SAMPLING_INTERVAL_PROPERTY_NAME = "samplingInterval";
    private static final String COUNT_TP_PROPERTY_NAME = "countTp";
    private static final String NUM_LOOP_PROPERTY_NAME = "numLoop";
    private static final String DATA_STORE_FILE_PROPERTY_NAME = "dbFile";
    private static final String LOG_LEVEL_PROPERTY_NAME = "logLevel";
    private static final String MIN_SUCCESS_RATE_FOR_PASS_PROPERTY_NAME = "minSuccessRate";
}
