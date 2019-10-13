package pers.yue.performance.runner.s3;

import pers.yue.performance.client.S3ClientHelper;
import pers.yue.performance.stat.RequestResult;
import pers.yue.performance.util.ThreadUtil;
import pers.yue.performance.runner.IteratorRunner;
import pers.yue.performance.runner.PerfRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by zhangyue58 on 2017/11/29
 */
public class DeleteObjectRunner extends AbstractObjectRunner implements Runnable, PerfRunner, IteratorRunner {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public DeleteObjectRunner(S3ClientHelper s3Helper, String bucketName, List<String> keys) {
        super(s3Helper, bucketName, keys);

        if(keys == null) {
            logger.error("keys is null");
            throw new RuntimeException("keys is null");
        }
    }

    public DeleteObjectRunner(S3ClientHelper s3Helper, String bucketName, Integer numLoop, Map<String, String> objectStore) {
        super(s3Helper, bucketName, numLoop, objectStore);

        if(keys == null) {
            logger.error("keys is null");
            throw new RuntimeException("keys is null");
        }
    }

    public RequestResult doAction(int count) {
        return null;
    }

    public RequestResult doAction(String key) {
        RequestResult requestResult;
        long start = System.currentTimeMillis();

        try {
            s3Helper.deleteObject(selectBucket(), key);

            long end = System.currentTimeMillis();
            requestResult = new RequestResult(start, end - start);
        } catch (RuntimeException e) {
            long end = System.currentTimeMillis();
            requestResult = new RequestResult(start, end - start, false);
        }

        if(objectStore != null) {
            try {
                objectStoreLocker.lock();
                objectStore.remove(key);
            } finally {
                objectStoreLocker.unlock();
            }
        }

        return requestResult;
    }
}
