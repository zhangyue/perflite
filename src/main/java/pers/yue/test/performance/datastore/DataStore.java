package pers.yue.test.performance.datastore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.exceptions.runtime.TestRunException;
import pers.yue.util.LogUtil;
import pers.yue.util.ThreadUtil;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * A key-value database that stores the test data information.
 * @param <K>
 * @param <V>
 */
public class DataStore<K, V> {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private Map<K, V> store;

    private String dataStorePath = "db";
    private String dataStoreFileName = "store.db";
    private File dataStoreFile;

    public DataStore() {
        init();
    }

    public DataStore(String dataStoreFileName) {
        this.dataStoreFileName = dataStoreFileName;
        init();
    }

    private void init() {
        dataStoreFile = new File(dataStorePath + File.separator + dataStoreFileName);
        store = new HashMap<>();
        load();
    }

    public File getStoreFile() {
        return dataStoreFile;
    }

    public Map<K, V> getStore() {
        return store;
    }

    private void load() {
        if(!dataStoreFile.exists()) {
            logger.info("Create data store at {}", dataStoreFile);

            dataStoreFile.getParentFile().mkdirs();
            try {
                dataStoreFile.createNewFile();
                return;
            } catch (IOException e) {
                logger.error("{} when load object store", e.getClass().getSimpleName(), e);
                throw new TestRunException(e);
            }
        }

        logger.info("Load data store from {}", dataStoreFile);
        try(
                FileInputStream fileIn = new FileInputStream(dataStoreFile);
                ObjectInputStream objectIn = new ObjectInputStream(fileIn)
        ) {
            store = (Map<K, V>) objectIn.readObject();
        } catch (EOFException e) {
            logger.info("Empty db file.");
        } catch (FileNotFoundException e) {
            logger.warn("File {} not found", dataStoreFile);
        } catch (Exception e) {
            logger.error("{} when load data store", e.getClass().getSimpleName(), e);
            throw new TestRunException(e);
        }
    }

    public void persist() {
        LogUtil.logAndSOutInfo(logger, "Persist data store into " + dataStoreFile);
        try(
                FileOutputStream fileOut = new FileOutputStream(dataStoreFile);
                ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)
        ) {
            objectOut.writeObject(store);
            LogUtil.logAndSOutInfo(logger, store.size() + " keys persisted into data store.");
        } catch (IOException e) {
            logger.error("{} when persist data store into {}", e.getClass().getSimpleName(), dataStoreFile, e);
            throw new TestRunException(e);
        }
    }
}
