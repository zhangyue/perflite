package pers.yue.test.demo.performance.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.test.demo.performance.client.DemoClient;
import pers.yue.test.performance.config.PerfConfig;
import pers.yue.test.performance.runner.IteratorRunner;
import pers.yue.test.performance.runner.LoopRunner;
import pers.yue.test.performance.stat.RequestResult;
import pers.yue.util.ThreadUtil;

/**
 * Demo DELETE runner
 *
 * Created by Zhang Yue on 6/8/2019
 */
public class DemoDeleteRunner extends AbstractDemoRunner implements LoopRunner, IteratorRunner {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public DemoDeleteRunner(DemoClient client, PerfConfig<String> config) {
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
        boolean success = true;
        long start = System.currentTimeMillis();
        long end;

        try {
            client().delete(key);
        } catch (Throwable e) {
            success = false;
        } finally {
            end = System.currentTimeMillis();
        }

        removeFromDataStore(key);

        return new RequestResult(start, end - start, success);
    }
}
