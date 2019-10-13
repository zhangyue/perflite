package pers.yue.performance.runner.s3;

import pers.yue.performance.client.S3ClientHelper;
import pers.yue.performance.stat.RequestResult;
import pers.yue.performance.util.Md5Util;
import pers.yue.performance.util.StreamUtil;
import pers.yue.performance.util.ThreadUtil;
import pers.yue.performance.runner.IteratorRunner;
import pers.yue.performance.runner.LoopRunner;
import pers.yue.performance.runner.PerfRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangyue58 on 2017/11/29
 */
public class GetObjectRunner extends AbstractObjectRunner implements Runnable, PerfRunner, LoopRunner, IteratorRunner {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    /**
     * With data verification on randomly selected objects.
     */
    public GetObjectRunner(S3ClientHelper s3Helper, String bucketName, Integer numLoop, Map<String, String> objectStore) {
        super(s3Helper, bucketName, numLoop, objectStore);
    }

    /**
     * With data verification on all objects
     */
    public GetObjectRunner(S3ClientHelper s3Helper, String bucketName, Map<String, String> objectStore) {
        super(s3Helper, bucketName, -1, objectStore);
    }

    /**
     * Without data verification on randomly selected objects
     */
    public GetObjectRunner(S3ClientHelper s3Helper, String bucketName, Integer numLoop, List<String> keys) {
        super(s3Helper, bucketName, numLoop, keys);
    }

    public RequestResult doAction(int count) {
        return null;
    }

    public RequestResult doAction(String key) {
        String md5Stored = null;
        if(objectStore != null) {
            md5Stored = objectStore.get(key);
        }

        RequestResult requestResult;
        long start = System.currentTimeMillis();

        try {
            InputStream contentIs = s3Helper.getObject(selectBucket(), key).getObjectContent();

            if(md5Stored != null && !md5Stored.isEmpty()) {
                String md5Get = Md5Util.getMd5(contentIs);
                if(!md5Get.equals(md5Stored)) {
                    throw new RuntimeException("MD5 not match: " + md5Get + " vs. " + md5Stored);
                }
            } else {
                StreamUtil.consumeStream(contentIs, downloadBuffer.get());
            }

            long end = System.currentTimeMillis();
            requestResult = new RequestResult(start, end - start);
        } catch (RuntimeException e) {
            long end = System.currentTimeMillis();
            requestResult = new RequestResult(start, end - start, false);
        }

        return requestResult;
    }
}
