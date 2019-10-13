package pers.yue.test.performance.data;

import pers.yue.test.util.FileTestUtil;
import pers.yue.test.util.Md5TestUtil;
import pers.yue.common.util.FileUtil;
import pers.yue.common.util.ThreadUtil;

import java.io.File;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Maintains a pool of data and generate them in advance of the test.
 *
 * Created by zhangyue182 on 2018/08/22
 */
public class PoolDataGenerator extends AbstractDataGenerator implements DataGenerator {
    private int poolSize;
    private DataInfo[] dataInfoArray;

    private boolean initiated = false;

    public PoolDataGenerator(long fileSizeMin, long fileSizeMax, int poolSize) {
        super(fileSizeMin, fileSizeMax);
        this.poolSize = poolSize;
    }

    public PoolDataGenerator(long fileSizeMin, long fileSizeMax, int poolSize, int prefixNum) {
        super(fileSizeMin, fileSizeMax, prefixNum);
        this.poolSize = poolSize;
    }

    private synchronized void init() {
        if(initiated) {
            return;
        }

        dataInfoArray = new DataInfo[poolSize];

        for(int i = 0; i < dataInfoArray.length; i++) {
            String key = generateFileName();

            long size = generateSize();
            // To make sure sizeMin and sizeMax are exactly covered besides the randomly generated sizes.
            if(i == 0) {
                size = fileSizeMin;
            } else if (i == 1) {
                size = fileSizeMax;
            }

            File file = FileTestUtil.generateFileContent(
                    FileTestUtil.createNewFileInTmp(PoolDataGenerator.class.getSimpleName(), key), size);
            dataInfoArray[i] = new DataInfo(file, Md5TestUtil.getMd5(file));
            ThreadUtil.sleep(1, TimeUnit.MILLISECONDS); // In case of two file names generated in the same millisecond.
        }
        initiated = true;
    }

    private synchronized void initExisting() {
        if(initiated) {
            return;
        }

        dataInfoArray = new DataInfo[poolSize];

        File[] files = FileUtil.listFilesInTmp(PoolDataGenerator.class.getSimpleName());
        for(int i = 0; i < files.length; i++) {
            dataInfoArray[i] = new DataInfo(files[i], Md5TestUtil.getMd5(files[i]));
        }

        initiated = true;
    }

    public DataInfo getDataInfo() {
        if(poolSize == -1) {
            initExisting();
            this.poolSize = dataInfoArray.length;
        } else {
            init();
        }

        return dataInfoArray[new Random().nextInt(dataInfoArray.length)];
    }
}
