package pers.yue.test.performance.data;

import pers.yue.test.util.FileTestUtil;
import pers.yue.test.util.Md5TestUtil;

import java.io.File;

/**
 * Generate data randomly. Each data is unique in this world.
 * This is not recommended for performance test as it causes extra overhead to the framework.
 * Do not use it if the test is done for data verification and client-side performance is not a big deal.
 *
 * Created by zhangyue182 on 2018/08/22
 */
public class RandomDataGenerator extends AbstractDataGenerator implements DataGenerator {
    public RandomDataGenerator(long fileSizeMin, long fileSizeMax) {
        super(fileSizeMin, fileSizeMax);
    }

    public DataInfo getDataInfo() {
        String key = generateFileName();

        File file = FileTestUtil.generateFileContent(
                FileTestUtil.createNewFileInTmp(RandomDataGenerator.class.getSimpleName(), key), generateSize()
        );

        String md5 = Md5TestUtil.getMd5(file);

        return new DataInfo(file, md5);
    }
}
