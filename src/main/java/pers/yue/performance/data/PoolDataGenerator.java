package pers.yue.performance.data;

import pers.yue.performance.util.FileUtil;
import pers.yue.performance.util.Md5Util;
import pers.yue.performance.util.ThreadUtil;

import java.io.File;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhangyue58 on 2018/08/22
 */
public class PoolDataGenerator extends AbstractDataGenerator implements DataGenerator {
    private int poolSize = 10;
    private DataInfo[] dataInfoArray;

    private boolean initiated = false;

    public PoolDataGenerator(long fileSizeMin, long fileSizeMax) {
        super(fileSizeMin, fileSizeMax);
    }

    public PoolDataGenerator(long fileSizeMin, long fileSizeMax, int poolSize) {
        super(fileSizeMin, fileSizeMax);
        this.poolSize = poolSize;

        init();
    }

    private synchronized void init() {
        if(initiated) {
            return;
        }

        dataInfoArray = new DataInfo[poolSize];

        for(int i = 0; i < dataInfoArray.length; i++) {
            String key = generateFileName();
            File file = FileUtil.generateFileContent(
                    FileUtil.createNewFileInTmp(PoolDataGenerator.class.getSimpleName(), key), generateSize());
            String md5 = Md5Util.getMd5(file);
            dataInfoArray[i] = new DataInfo(file, md5);
            ThreadUtil.sleep(1, TimeUnit.MILLISECONDS); // In case of two file names generated in the same millisecond.
        }
        initiated = true;
    }

    public DataInfo getDataInfo() {
        return dataInfoArray[new Random().nextInt(dataInfoArray.length)];
    }
}
