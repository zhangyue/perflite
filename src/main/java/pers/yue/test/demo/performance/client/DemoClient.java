package pers.yue.test.demo.performance.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.common.util.StreamUtil;
import pers.yue.common.util.ThreadUtil;

import java.io.File;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Client of your SUT (System Under Test).
 *
 * Created by Zhang Yue on 6/8/2019
 */
public class DemoClient {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private String endpoint;

    public DemoClient(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * PUT method API wrapper
     * @param key Key of data to PUT.
     * @param contentFile Data to be PUT>
     */
    public void put(String key, File contentFile) {
        logger.info("[PUT] {} - {} | Length: {}", endpoint, key, contentFile.length());
        ThreadUtil.sleep(new Random().nextInt(100), TimeUnit.MILLISECONDS);
    }

    /**
     * GET method API wrapper
     * @param key Key of data to GET.
     * @return Data of the key.
     */
    public InputStream get(String key) {
        logger.info("[GET] {} - {}", endpoint, key);
        ThreadUtil.sleep(new Random().nextInt(10), TimeUnit.MILLISECONDS);
        return StreamUtil.convertStringToInputStream("foo-bar-" + System.currentTimeMillis());
    }

    /**
     * DELETE method API wrapper
     * @param key Key of data to DELETE.
     */
    public void delete(String key) {
        logger.info("[DELETE] {} - {}", endpoint, key);
        ThreadUtil.sleep(new Random().nextInt(60), TimeUnit.MILLISECONDS);
    }
}
