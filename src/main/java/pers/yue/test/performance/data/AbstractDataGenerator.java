package pers.yue.test.performance.data;

import pers.yue.common.util.StringUtil;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Base class of data generator.
 *
 * Created by zhangyue182 on 2018/08/22
 */
public abstract class AbstractDataGenerator implements DataGenerator {
    long fileSizeMin;
    long fileSizeMax;

    private String fileTag = "objperf";
    private int numPrefix = 0;
    private List<String> prefixList = new ArrayList<>();
    private static final int NUM_ALPHANUM = 62;

    private long shrinkOffset = 0;
    private int shrinkTimes = 0;

    AbstractDataGenerator(long fileSizeMin, long fileSizeMax) {
        this.fileSizeMin = fileSizeMin;
        this.fileSizeMax = fileSizeMax;
    }

    AbstractDataGenerator(long fileSizeMin, long fileSizeMax, int numPrefix) {
        this.fileSizeMin = fileSizeMin;
        this.fileSizeMax = fileSizeMax;
        if (numPrefix < NUM_ALPHANUM * NUM_ALPHANUM) {
            this.numPrefix = numPrefix;
        } else {
            this.numPrefix = NUM_ALPHANUM * NUM_ALPHANUM;
        }
        prefixList = generatePrefix();
    }

    public void setFileTag(String fileTag) {
        this.fileTag = fileTag;
    }

    public void setShrinkOffset(long shrinkOffset) {
        this.shrinkOffset = shrinkOffset;
    }

    public void setShrinkTimes(int shrinkTimes) {
        this.shrinkTimes = shrinkTimes;
    }

    String generateFileName() {
        String dateTime = new SimpleDateFormat("YYYYMMddHHmmssSSS").format(new Date());
        if ( this.numPrefix > 0) {
            return prefixList.get(new Random().nextInt(numPrefix)) + "-" + fileTag + "-" + dateTime;
        } else {
            return fileTag + "-" + dateTime;
        }
    }

    long generateSize() {
        long randomSize = new Random().nextInt((int)(fileSizeMax - fileSizeMin + 1));

        long generatedSize = fileSizeMin + randomSize;

        if(shrinkOffset != 0 && shrinkTimes != 0) {
            if(fileSizeMin + randomSize < shrinkOffset) {
                generatedSize = fileSizeMin + randomSize / shrinkTimes;
            }
        }

        return generatedSize;
    }

    private List<String> generatePrefix() {
        List<String> prefix = new ArrayList<>();
        for (int i=0; i < this.numPrefix; i++) {
            prefix.add(chooseCharacter());
        }
        return prefix;
    }

    private String chooseCharacter() {
        if ( this.numPrefix > NUM_ALPHANUM ) {
            return StringUtil.generateRandomAlphaNumString(2);
        } else {
            return StringUtil.generateRandomAlphaNumString(1);
        }
    }
}
