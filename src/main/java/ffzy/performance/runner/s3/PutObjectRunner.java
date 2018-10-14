package ffzy.performance.runner.s3;

import ffzy.performance.client.S3ClientHelper;
import ffzy.performance.data.DataGenerator;
import ffzy.performance.data.DataInfo;
import ffzy.performance.stat.RequestResult;
import ffzy.performance.util.ThreadUtil;
import ffzy.performance.runner.LoopRunner;
import ffzy.performance.runner.PerfRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * Created by zhangyue58 on 2017/11/29
 */
public class PutObjectRunner extends AbstractObjectRunner implements Runnable, PerfRunner, LoopRunner {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private DataGenerator dataGenerator;

    public PutObjectRunner(S3ClientHelper s3Helper, String bucketName, int numLoop,
                           DataGenerator dataGenerator, Map<String, String> objectStore) {
        super(s3Helper, bucketName, numLoop, objectStore);
        this.dataGenerator = dataGenerator;

        if(numLoop <= 0) {
            logger.error("Invalid numLoop: {}", numLoop);
            throw new RuntimeException("Invalid numLoop: " + numLoop);
        }
    }

    public RequestResult doAction(int count) {
        DataInfo dataInfo = dataGenerator.getDataInfo();
        File file = dataInfo.getSourceFile();
        String key = generateUniqueKey(file.getName());

        RequestResult requestResult;
        long start = System.currentTimeMillis();

        try {
            s3Helper.putObject(selectBucket(), key, file);

            long end = System.currentTimeMillis();
            requestResult = new RequestResult(start, end - start);
        } catch(RuntimeException e) {
            long end = System.currentTimeMillis();
            requestResult = new RequestResult(start, end - start, false);
        }

        try {
            objectStoreLocker.lock();
            objectStore.put(key, dataInfo.getMd5());
        } finally {
            objectStoreLocker.unlock();
        }

        return requestResult;
    }

    public RequestResult doAction(String key) {
        return null;
    }
}
