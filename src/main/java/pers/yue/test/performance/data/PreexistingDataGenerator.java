package pers.yue.test.performance.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.test.util.Md5TestUtil;
import pers.yue.util.ThreadUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Create a pool of data from existing files.
 *
 * Created by zhangyue182 on 2019/07/10
 */
public class PreexistingDataGenerator implements DataGenerator {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private List<DataInfo> dataInfoList = new ArrayList<>();

    private String pathToDataFile;

    public PreexistingDataGenerator(String pathToDataFile) {
        this.pathToDataFile = pathToDataFile;
        init();
    }

    private synchronized void init() {
        File dataFile = new File(pathToDataFile);
        File[] dataFilesAndDirs;
        if(dataFile.isFile()) {
            dataFilesAndDirs = new File[1];
            dataFilesAndDirs[0] = dataFile;
        } else {
            dataFilesAndDirs = dataFile.listFiles();
        }
        if(dataFilesAndDirs == null) {
            logger.error("No data files found in {}.", pathToDataFile);
            return;
        }
        for(File dataFileOrDir : dataFilesAndDirs) {
            if(dataFileOrDir.isFile()) {
                dataInfoList.add(new DataInfo(dataFileOrDir, Md5TestUtil.getMd5(dataFileOrDir)));
            }
        }
    }

    public void setFileTag(String fileTag) {
        logger.warn("Method {} is not supported by class {}.", ThreadUtil.getMethodName(), ThreadUtil.getClassName());
    }

    public void setShrinkOffset(long shrinkOffset) {
        logger.warn("Method {} is not supported by class {}.", ThreadUtil.getMethodName(), ThreadUtil.getClassName());
    }

    public void setShrinkTimes(int shrinkTimes) {
        logger.warn("Method {} is not supported by class {}.", ThreadUtil.getMethodName(), ThreadUtil.getClassName());
    }

    public DataInfo getDataInfo() {
        return dataInfoList.get(new Random().nextInt(dataInfoList.size()));
    }
}
