package ffzy.performance;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.jcloud.Constants;
import com.jcloud.JssTestManager;
import com.jcloud.clienthelper.S3ClientHelper;
import com.jcloud.test.performance.data.DataGenerator;
import com.jcloud.test.performance.data.PoolDataGenerator;
import com.jcloud.test.performance.data.RandomDataGenerator;
import com.jcloud.test.performance.report.CsvReporter;
import com.jcloud.test.performance.report.Reporter;
import com.jcloud.test.performance.runner.*;
import com.jcloud.testmanager.TestCase;
import com.jcloud.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.jcloud.util.FileUtil.ByteUnit.MB;

// TODO

/**
 * Created by zhangyue58 on 2017/11/29
 */
public class ObjectPerfTest extends JssTestManager {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private String myBucketName;

    public static final long DEFAULT_SIZE_MIN = 1L;
    public static final long DEFAULT_SIZE_MAX = 2 * MB;
    public static final int DEFAULT_NUM_LOOP = 10;
    public static final int DEFAULT_NUM_THREAD = 5;
    public static final String DEFAULT_PATH_TO_REPORTER_FILE = "report" + File.separator + "perf_report.csv";

    public static final String SIZE_MIN_PROPERTY_NAME = "sizeMin";
    public static final String SIZE_MAX_PROPERTY_NAME = "sizeMax";
    public static final String NUM_LOOP_PROPERTY_NAME = "numLoop";
    public static final String NUM_THREAD_PROPERTY_NAME = "numThread";
    public static final String SAMPLING_INTERVAL = "samplingInterval";
    public static final String DATA_STORE_FILE = "dbFile";

    private long sizeMin = DEFAULT_SIZE_MIN;
    private long sizeMax = DEFAULT_SIZE_MAX;
    private int numLoop = DEFAULT_NUM_LOOP;
    private int numThread = DEFAULT_NUM_THREAD;
    private int samplingInterval = AbstractObjectRunner.DEFAULT_STAT_INTERVAL;
    private String pathToReporterFile = DEFAULT_PATH_TO_REPORTER_FILE;

    private Reporter reporter;

    String dataStorePath = "db";
    String dataStoreFile = "object_store.db";
    String dataStorePathToFile;

    File tmpDir;

    @BeforeClass(alwaysRun = true)
    public void setupObjectPerfTest() {
        String sizeMinProperty = System.getProperty(SIZE_MIN_PROPERTY_NAME);
        String sizeMaxProperty = System.getProperty(SIZE_MAX_PROPERTY_NAME);
        String numLoopProperty = System.getProperty(NUM_LOOP_PROPERTY_NAME);
        String numThreadProperty = System.getProperty(NUM_THREAD_PROPERTY_NAME);
        String samplingIntervalProperty = System.getProperty(SAMPLING_INTERVAL);
        String dbFileProperty = System.getProperty(DATA_STORE_FILE);

        try {
            sizeMin = parseProperty(sizeMin, sizeMinProperty);
            sizeMax = parseProperty(sizeMax, sizeMaxProperty);
            numLoop = parseProperty(numLoop, numLoopProperty);
            numThread = parseProperty(numThread, numThreadProperty);
            samplingInterval = parseProperty(samplingInterval, samplingIntervalProperty);
            dataStoreFile = parseProperty(dataStoreFile, dbFileProperty);
        } catch (RuntimeException e) {
            logger.error("Exception when parsing system property.", e);
            throw e;
        }

        dataStorePathToFile = dataStorePath + File.separator + dataStoreFile;

        logger.info("======== datastore: {} ========", dataStorePathToFile);
        logger.info("======== sizeMin: {} ========", sizeMin);
        logger.info("======== sizeMax: {} ========", sizeMax);
        logger.info("======== numLoop: {} ========", numLoop);
        logger.info("======== samplingInterval: {} ========", samplingInterval);

        user1.getBucketManager().prepareDefaultBucket();

        myBucketName = user1.getBucketNameManager().getNextBucketName();
        user1.getBucketManager().prepareBucket(myBucketName, Constants.PREPARE_BUCKET_MANNER_REUSE, Constants.CLEANUP_SCOPE_NONE);

        new File(dataStorePath).mkdirs();
        tmpDir = new File(Constants.TMP_DIRECTORY_PATH + File.separator + this.getClass().getSimpleName());
        tmpDir.mkdirs();

        reporter = new CsvReporter(new File(pathToReporterFile));

        user1.s3().setVerbose(false);
        user2.s3().setVerbose(false);
    }

    @AfterClass(alwaysRun = true)
    public void cleanupObjectPerfTest() {
        user1.s3().setVerbose(true);
        user2.s3().setVerbose(true);
    }

    private String parseProperty(String defaultValue, String property) {
        if (property == null || property.isEmpty()) {
            return defaultValue;
        }
        return property;
    }

    private int parseProperty(int defaultValue, String property) {
        if (property == null || property.isEmpty()) {
            return defaultValue;
        }
        return Integer.valueOf(property);
    }

    private long parseProperty(long defaultValue, String property) {
        if (property == null || property.isEmpty()) {
            return defaultValue;
        }
        return Long.valueOf(property);
    }

    @Test(groups = "put_object")
    public void runPerfPutObject() {
        int poolSize = 10;
        DataGenerator dataGenerator = new PoolDataGenerator(sizeMin, sizeMax, poolSize);

        perfPutObject(user1.s3(), dataGenerator);
    }

    /**
     * Note that this test has extra mean time to generate random data file in each request loop.
     */
    @Test(groups = "put_object")
    public void runPerfPutObjectRandomData() {
        DataGenerator dataGenerator = new RandomDataGenerator(sizeMin, sizeMax);

        perfPutObject(user1.s3(), dataGenerator);
    }

    @Test(priority = 1, groups = "put_object_with_bucket_policy")
    public void runPerfPutObjectWithBucketPolicy() {
        setBucketPolicy();

        int poolSize = 10;
        DataGenerator dataGenerator = new PoolDataGenerator(sizeMin, sizeMax, poolSize);

        perfPutObject(user2.s3(), dataGenerator);
    }

    private void perfPutObject(S3ClientHelper s3Helper, DataGenerator dataGenerator) {
        Map<String, String> objectStore = loadObjectStore(dataStorePathToFile);

        PerfRunner putObjectRunner = new PutObjectRunner(s3Helper, myBucketName, numLoop, dataGenerator, objectStore);

        putObjectRunner.setStatInterval(samplingInterval);
        putObjectRunner.setReporter(reporter);

        Launcher launcher = new Launcher(putObjectRunner, numThread, reporter);
        launcher.launch();

        persistObjectStore(objectStore, dataStorePathToFile);
    }

    private void setBucketPolicy() {
        List<String> accounts = new ArrayList<>();
        accounts.add(user2.getUserId());
        List<String> actions = new ArrayList<>();
        actions.add(Constants.ACTION_PUT_OBJECT);
        List<String> resources = new ArrayList<>();
        resources.add("arn:aws:s3:::" + myBucketName + "/*");

        user1.s3().setBucketPolicy("testActionPutObject",
                Constants.EFFECT_ALLOW, accounts, actions, resources, null, myBucketName);
    }

    @Test(priority = 2)
    public void runPerfGetObject() {
        Map<String, String> objectStore = loadObjectStore(dataStorePathToFile);

        PerfRunner getObjectRunner = new GetObjectRunner(user1.s3(), myBucketName, numLoop, new ArrayList<>(objectStore.keySet()));

        getObjectRunner.setStatInterval(samplingInterval);
        getObjectRunner.setReporter(reporter);

        Launcher launcher = new Launcher(getObjectRunner, numThread, reporter);
        launcher.launch();
    }

    @Test(priority = 2)
    public void runPerfGetObjectVerifyData() {
        Map<String, String> objectStore = loadObjectStore(dataStorePathToFile);

        PerfRunner getObjectRunner = new GetObjectRunner(user1.s3(), myBucketName, numLoop, objectStore);

        getObjectRunner.setStatInterval(samplingInterval);
        getObjectRunner.setReporter(reporter);

        Launcher launcher = new Launcher(getObjectRunner, numThread, reporter);
        launcher.launch();
    }

    @Test(priority = 3)
    public void runPerfGetOldObject() {
        for(Bucket bucket : user1.s3().getService()) {
            if(!bucket.getName().startsWith("automation")) {
                continue;
            }

            List<S3ObjectSummary> objectSummaries = user1.s3().listAllObjectsV2(bucket.getName());

            List<String> keys = objectSummariesToKeys(objectSummaries);

            logger.info("{} objects in bucket {}", keys.size(), bucket.getName());

            if(keys.size() == 0) {
                continue;
            }

            PerfRunner getObjectRunner = new GetObjectRunner(user1.s3(), bucket.getName(), numLoop, keys);

            getObjectRunner.setStatInterval(samplingInterval);
            getObjectRunner.setReporter(reporter);

            Launcher launcher = new Launcher(getObjectRunner, numThread, reporter);
            launcher.launch();
        }
    }

    private List<String> objectSummariesToKeys(List<S3ObjectSummary> objectSummaries) {
        List<String> keys = new ArrayList<>();
        for (S3ObjectSummary objectSummary : objectSummaries) {
            if(objectSummary.getKey().contains("Flowers_and_Trees_1932")
                    || objectSummary.getKey().contains("50mb")) {
                continue;
            }
            keys.add(objectSummary.getKey());
        }
        return keys;
    }

    @Test(priority = 5)
    public void runPerfDeleteObject() {
        Map<String, String> objectStore = loadObjectStore(dataStorePathToFile);

        DeleteObjectRunner deleteObjectRunner = new DeleteObjectRunner(user1.s3(), myBucketName, numLoop, objectStore);

        deleteObjectRunner.setStatInterval(samplingInterval);
        deleteObjectRunner.setReporter(reporter);

        Launcher launcher = new Launcher(deleteObjectRunner, numThread, reporter);
        launcher.launch();

        persistObjectStore(objectStore, dataStorePathToFile);
    }


    @Test(groups = "fullcycle_object")
    public void runPerfFullcycleObject() {
        int poolSize = 20;
        DataGenerator dataGenerator = new PoolDataGenerator(sizeMin, sizeMax, poolSize);
        // For chunk file, reduce file size by 10 to save DS chunk store.
        dataGenerator.setShrinkOffset(1 * MB);
        dataGenerator.setShrinkTimes(100);

        PerfRunner fullcycleObjectRunner = new FullcycleObjectRunner(user1.s3(), myBucketName, numLoop, dataGenerator);

        fullcycleObjectRunner.setStatInterval(samplingInterval);
        fullcycleObjectRunner.setReporter(reporter);

        Launcher launcher = new Launcher(fullcycleObjectRunner, numThread, reporter);
        launcher.launch();
    }

    private Map<String, String> loadObjectStore(String objectStorePathToFile) {
        Map<String, String> objectStore = new TreeMap<>();
        logger.info("Load object store from {}", objectStorePathToFile);
        try(
                FileInputStream fileIn = new FileInputStream(objectStorePathToFile);
                ObjectInputStream objectIn = new ObjectInputStream(fileIn)
        ) {
            objectStore = (Map<String, String>) objectIn.readObject();
        } catch (FileNotFoundException e) {
            logger.warn("File {} not found", objectStorePathToFile);
        } catch (IOException e) {
            logger.error("{} when load object store", e.getClass().getSimpleName(), e);
            TestCase.fail("Test fails.");
        } catch (ClassNotFoundException e) {
            logger.error("{} when load object store", e.getClass().getSimpleName(), e);
            TestCase.fail("Test fails.");
        }

        return objectStore;
    }

    private void persistObjectStore(Map<String, String> objectStore, String objectStorePathToFile) {
        logger.info("Persist object store into {}.", objectStorePathToFile);
        try(
                FileOutputStream fileOut = new FileOutputStream(objectStorePathToFile);
                ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)
        ) {
            objectOut.writeObject(objectStore);
        } catch (IOException e) {
            logger.error("{} when persist object store into {}", e.getClass().getSimpleName(), objectStorePathToFile, e);
            TestCase.fail("Test fails.");
        }
    }
}
