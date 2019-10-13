package pers.yue.performance.data;

import pers.yue.performance.util.FileUtil;
import pers.yue.performance.util.Md5Util;

import java.io.File;

/**
 * Created by zhangyue58 on 2018/08/22
 */
public class RandomDataGenerator extends AbstractDataGenerator implements DataGenerator {
    public RandomDataGenerator(long fileSizeMin, long fileSizeMax) {
        super(fileSizeMin, fileSizeMax);
    }

    public DataInfo getDataInfo() {
        String key = generateFileName();

        File file = FileUtil.generateFileContent(
                FileUtil.createNewFileInTmp(RandomDataGenerator.class.getSimpleName(), key), generateSize()
        );

        String md5 = Md5Util.getMd5(file);

        return new DataInfo(file, md5);
    }
}
