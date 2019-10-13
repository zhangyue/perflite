package pers.yue.test.demo.performance.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.test.demo.performance.client.DemoClient;
import pers.yue.test.performance.config.PerfConfig;
import pers.yue.test.performance.data.DataGenerator;
import pers.yue.test.performance.data.DataInfo;
import pers.yue.test.performance.runner.LoopRunner;
import pers.yue.test.performance.stat.RequestResult;
import pers.yue.util.ThreadUtil;

import java.io.File;

/**
 * Demo PUT runner
 *
 * Created by Zhang Yue on 6/8/2019
 */
public class DemoPutRunner extends AbstractDemoRunner implements LoopRunner {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    /**
     * DataGenerator object that helps generate test data.
     */
    private DataGenerator dataGenerator;

    /**
     * Constructor of DemoPutRunner
     * @param client Client of SUT.
     * @param dataGenerator DataGenerator object that helps generate test data.
     * @param config Perf test config.
     */
    public DemoPutRunner(DemoClient client, DataGenerator dataGenerator, PerfConfig<String> config) {
        super(client, config);
        this.dataGenerator = dataGenerator;
        checkNumLoop();
    }

    /**
     * The actual method to do PUT action.
     * @return RequestResult object that carries the result of the test request, e.g. success or fail, latency, etc.
     */
    @Override
    public RequestResult doAction() {
        DataInfo dataInfo = dataGenerator.getDataInfo();
        File file = dataInfo.getSourceFile();
        String key = generateUniqueKey(file.getName());

        boolean success = true;
        long start = System.currentTimeMillis();
        long end;

        try {
            client().put(key, file);
        } catch(RuntimeException e) {
            success = false;
        } finally {
            end = System.currentTimeMillis();
        }

        addToDataStore(key, dataInfo.getMd5());

        return new RequestResult(start, end - start, success);
    }

    /**
     * Not used.
     */
    @Override
    public RequestResult doAction(String key) {
        return null;
    }
}
