package pers.yue.test.performance.config;

import java.util.List;
import java.util.Set;

/**
 * The interface for filtering the test keys with certain policy.
 *
 * Created by linjiangui on 2019/04/02
 */
public interface Filter {

    /**
     * Implement this method to filter the keys as you wish.
     * @return The keys that will finally be used by the test, which could be a subset of all the keys recorded in data store.
     */
    List<String> filterKeys();

    Filter withKeys(List<String> keys);

    Filter withKeys(Set<String> keys);

}
