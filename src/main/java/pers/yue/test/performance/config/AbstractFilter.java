package pers.yue.test.performance.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by zhangyue182 on 2019/08/21
 */
public abstract class AbstractFilter implements Filter {
    private List<String> keys;

    public AbstractFilter(List<String> keys) {
        this.keys = keys;
    }

    public List<String> getKeys() {
        return keys;
    }

    public Filter withKeys(List<String> keys) {
        this.keys = keys;
        return this;
    }

    public Filter withKeys(Set<String> keys) {
        this.keys = new ArrayList<>(keys);
        return this;
    }
}
