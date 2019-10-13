package pers.yue.test.demo.performance.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.test.demo.performance.client.DemoClient;
import pers.yue.test.performance.config.PerfConfig;
import pers.yue.test.performance.runner.IteratorRunner;
import pers.yue.test.performance.runner.LoopRunner;
import pers.yue.test.performance.stat.RequestResult;
import pers.yue.test.testcase.TestCase;
import pers.yue.test.util.Md5TestUtil;
import pers.yue.common.util.ThreadUtil;

import java.io.InputStream;

/**
 * Demo GET runner
 *
 * Created by Zhang Yue on 6/8/2019
 */
public class DemoGetRunner extends AbstractDemoRunner implements LoopRunner, IteratorRunner {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public DemoGetRunner(DemoClient client, PerfConfig<String> config) {
        super(client, config);
        checkNumLoop();
    }

    /**
     * Not used.
     */
    @Override
    public RequestResult doAction() {
        return null;
    }

    /**
     * The actual method to do GET action.
     * @param key The key to get.
     * @return The data retrieved from the SUT.
     */
    @Override
    public RequestResult doAction(String key) {
        String md5Stored = null;
        if(dataStore != null) {
            md5Stored = dataStore.getStore().get(key);
        }

        long start = System.currentTimeMillis();
        long end;

        try {
            InputStream content = client().get(key);
            end = System.currentTimeMillis();

            if(getConfig().getKeysAccessMode() == PerfConfig.KeysAccessMode.dataStore
                    && md5Stored != null && !md5Stored.isEmpty()) {
                TestCase.assertEquals(Md5TestUtil.getMd5(content, getLogLevel()), md5Stored, "MD5 not match");
            }

        } catch (Throwable e) {
            end = System.currentTimeMillis();
            return new RequestResult(start, end - start, false);
        }

        return new RequestResult(start, end - start);
    }
}
