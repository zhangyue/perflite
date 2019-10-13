package pers.yue.test.demo.performance.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.test.demo.performance.client.DemoClient;
import pers.yue.test.performance.config.PerfConfig;
import pers.yue.test.performance.runner.AbstractPerfRunner;
import pers.yue.test.performance.runner.PerfRunner;
import pers.yue.util.ThreadUtil;

/**
 * The abstract class that holds the member variables that are common to different test runners. E.g. Client of your SUT.
 *
 * Created by Zhang Yue on 6/8/2019
 */
abstract class AbstractDemoRunner extends AbstractPerfRunner<String> implements Runnable, PerfRunner {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    /**
     * Client of SUT (System Under Test)
     */
    private DemoClient client;

    AbstractDemoRunner(DemoClient client, PerfConfig<String> config) {
        super(config);
        this.client = client;
    }

    DemoClient client() {
        return client;
    }

    protected void preRun() {
        logger.info("{} in {}", ThreadUtil.getMethodName(), ThreadUtil.getClassName());
    }

    protected void postRun() {
        logger.info("{} in {}", ThreadUtil.getMethodName(), ThreadUtil.getClassName());
    }
}
