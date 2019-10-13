package pers.yue.test.demo.performance.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import pers.yue.test.demo.performance.client.DemoClient;
import pers.yue.test.demo.performance.runner.DemoDeleteRunner;
import pers.yue.test.demo.performance.runner.DemoGetRunner;
import pers.yue.test.demo.performance.runner.DemoPutRunner;
import pers.yue.test.performance.PerfTestCore;
import pers.yue.test.performance.config.Filter;
import pers.yue.test.performance.config.PercentFilter;
import pers.yue.test.performance.data.DataGenerator;
import pers.yue.test.performance.data.PoolDataGenerator;
import pers.yue.test.performance.stat.StatInfo;
import pers.yue.util.ThreadUtil;

import static pers.yue.util.FileUtil.MB;
import static pers.yue.util.PropertiesUtil.parseAlternativeSizeProperty;
import static pers.yue.util.PropertiesUtil.parseProperty;

/**
 * Demo test class of implementing performance test tool with the performance test core module.
 *
 * Created by Zhang Yue on 6/8/2019
 */
public class DemoPerfTest extends DemoTestBase {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    /**
     * The PerfTestBase instance which holds all the properties that are required by performance test.
     */
    private PerfTestCore<String> perfTest = new PerfTestCore<>();

    private static final long DEFAULT_SIZE_MIN = 1L;
    private static final long DEFAULT_SIZE_MAX = 2 * MB;
    private static final int DEFAULT_DATA_POOL_SIZE = 10;

    private static final String SIZE_MIN_PROPERTY_NAME = "sizeMin";
    private static final String SIZE_MAX_PROPERTY_NAME = "sizeMax";
    private static final String SIZE_PROPERTY_NAME = "size";
    private static final String DATA_POOL_SIZE_PROPERTY_NAME = "dataPoolSize";

    /**
     * Minimum size of test data.
     */
    private long sizeMin = DEFAULT_SIZE_MIN;

    /**
     * Maximum size of test data.
     */
    private long sizeMax = DEFAULT_SIZE_MAX;

    /**
     * Size of test data pool. Each file in this pool is unique.
     */
    private int dataPoolSize = DEFAULT_DATA_POOL_SIZE;

    @BeforeClass(alwaysRun = true)
    public void setupDemoPerfTest() {
        String sizeMinProperty = System.getProperty(SIZE_MIN_PROPERTY_NAME);
        String sizeMaxProperty = System.getProperty(SIZE_MAX_PROPERTY_NAME);
        String sizeProperty = System.getProperty(SIZE_PROPERTY_NAME);
        String dataPoolSizeProperty = System.getProperty(DATA_POOL_SIZE_PROPERTY_NAME);

        try {
            sizeMin = parseAlternativeSizeProperty(sizeMin, sizeMinProperty, sizeProperty);
            sizeMax = parseAlternativeSizeProperty(sizeMax, sizeMaxProperty, sizeProperty);
            dataPoolSize = parseProperty(dataPoolSize, dataPoolSizeProperty);
        } catch (RuntimeException e) {
            logger.error("Exception when parsing system property.", e);
            throw e;
        }

        logger.info("======== sizeMin: {} ========", sizeMin);
        logger.info("======== sizeMax: {} ========", sizeMax);
    }

    @Test
    public void runPerfPut() {
        DataGenerator dataGenerator = new PoolDataGenerator(sizeMin, sizeMax, dataPoolSize);
        StatInfo resultStat = perfTest.launchRunner(new DemoPutRunner(new DemoClient(endpoint), dataGenerator, perfTest.getConfig()));
        perfTest.getDataStore().persist();
        perfTest.assertResult(resultStat);
    }

    @Test
    public void runPerfGet() {
//        StatInfo resultStat = perfTest.launchRunner(new DemoGetRunner(new DemoClient(endpoint), perfTest.getConfig().withKeys()));
        Filter filter = new PercentFilter<>(perfTest.getConfig().withKeyList().getKeys(), 10);
        StatInfo resultStat = perfTest.launchRunner(new DemoGetRunner(new DemoClient(endpoint), perfTest.getConfig().withKeyList().withFilter(filter)));
        perfTest.assertResult(resultStat);
    }

    @Test
    public void runPerfGetVerifyData() {
        StatInfo resultStat = perfTest.launchRunner(new DemoGetRunner(new DemoClient(endpoint), perfTest.getConfig().withDataStore()));
        perfTest.assertResult(resultStat);
    }

    @Test
    public void runPerfDelete() {
        StatInfo resultStat = perfTest.launchRunner(new DemoDeleteRunner(new DemoClient(endpoint), perfTest.getConfig()));
        perfTest.getDataStore().persist();
        perfTest.assertResult(resultStat);
    }
}
