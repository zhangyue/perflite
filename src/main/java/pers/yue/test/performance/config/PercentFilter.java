package pers.yue.test.performance.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.exceptions.runtime.TestRunException;
import pers.yue.test.performance.datastore.DataStore;
import pers.yue.util.ThreadUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

/**
 * Filter the keys by randomly select a percentage of keys from all the keys.
 * @param <T>
 *
 * Created by linjiangui on 3/31/2019
 */
public class PercentFilter <T> extends AbstractFilter implements Filter {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private int percentage;

    public PercentFilter(List<String> keys, int percent) {
        super(keys);
        init(null, keys, percent);
    }

    public PercentFilter(DataStore<String, T> dataStore, int percent) {
        super(new ArrayList<>(dataStore.getStore().keySet()));
        init(dataStore, null, percent);
    }

    private void init(DataStore<String, T> dataStore, List<String> keys, int percent) {
        if ((keys == null || keys.size() == 0)
                && (dataStore == null || dataStore.getStore().size() == 0)){
            logger.error("dataStore and inputKeys are null");
            throw new TestRunException("dataStore and inputKeys are null");
        }

        this.percentage = percent;
    }

    @Override
    public List<String> filterKeys() {
        if (percentage < 0 || percentage > 90) {
            logger.error("invalid percentage param: {}", percentage);
            throw new TestRunException("invalid percentage params");
        }

        HashSet<String> keys = new HashSet<>();
        int keyCount = getKeys().size();

        Random r = new Random();
        while (keys.size() < keyCount * percentage / 100) {
            keys.add(getKeys().get(r.nextInt(keyCount)));
        }

        return new ArrayList<>(keys);
    }
}
