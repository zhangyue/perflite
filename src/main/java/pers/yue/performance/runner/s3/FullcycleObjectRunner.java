package pers.yue.performance.runner.s3;

import pers.yue.performance.client.S3ClientHelper;
import pers.yue.performance.data.DataGenerator;
import pers.yue.performance.data.DataInfo;
import pers.yue.performance.stat.RequestResult;
import pers.yue.performance.util.Md5Util;
import pers.yue.performance.util.StreamUtil;
import pers.yue.performance.util.ThreadUtil;
import pers.yue.performance.runner.LoopRunner;
import pers.yue.performance.runner.PerfRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by zhangyue58 on 2018/08/24
 */
public class FullcycleObjectRunner extends AbstractObjectRunner implements Runnable, PerfRunner, LoopRunner {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private DataGenerator dataGenerator;

    public FullcycleObjectRunner(S3ClientHelper s3Helper, String bucketName, int numLoop, DataGenerator dataGenerator) {
        super(s3Helper, bucketName, numLoop);
        this.dataGenerator = dataGenerator;

        if(numLoop <= 0) {
            logger.error("Invalid numLoop: {}", numLoop);
            throw new RuntimeException("Invalid numLoop: " + numLoop);
        }
    }

    public RequestResult doAction(int count) {
        String bucketName = selectBucket();

        DataInfo dataInfo = dataGenerator.getDataInfo();
        File file = dataInfo.getSourceFile();

        String key = generateUniqueKey(file.getName());

        RequestResult requestResult;
        long start = System.currentTimeMillis();

        try {
            s3Helper.putObject(bucketName, key, file);

            String isVerifyData = System.getProperty("verify");
            if (isVerifyData == null || isVerifyData == "false") {
                StreamUtil.convertStreamToByteArray(s3Helper.getObject(bucketName, key).getObjectContent());
            } else {
                String md5Get = Md5Util.getMd5(s3Helper.getObject(bucketName, key).getObjectContent());
                if(!md5Get.equals(dataInfo.getMd5())) {
                    throw new RuntimeException("MD5 not match: " + md5Get + " vs. " + dataInfo.getMd5());
                }
            }

            s3Helper.deleteObject(bucketName, key);

            long end = System.currentTimeMillis();
            requestResult = new RequestResult(start, end - start);
        } catch (RuntimeException e) {
            long end = System.currentTimeMillis();
            requestResult = new RequestResult(start, end - start, false);
        }

        return requestResult;
    }

    public RequestResult doAction(String key) {
        return null;
    }
}
