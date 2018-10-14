package ffzy.performance.data;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * Created by zhangyue58 on 2018/08/22
 */
public abstract class AbstractDataGenerator implements DataGenerator {
    protected long fileSizeMin;
    protected long fileSizeMax;

    protected String fileNamePrefix = "objperf";

    protected long shrinkOffset = 0;
    protected int shrinkTimes = 0;

    public AbstractDataGenerator(long fileSizeMin, long fileSizeMax) {
        this.fileSizeMin = fileSizeMin;
        this.fileSizeMax = fileSizeMax;
    }

    public void setShrinkOffset(long shrinkOffset) {
        this.shrinkOffset = shrinkOffset;
    }

    public void setShrinkTimes(int shrinkTimes) {
        this.shrinkTimes = shrinkTimes;
    }

    protected String generateFileName() {
        String dateTime = new SimpleDateFormat("YYYYMMddHHmmssSSS").format(new Date());

        return fileNamePrefix + "-" + dateTime;
    }

    protected long generateSize() {
        long randomSize = new Random().nextInt((int)(fileSizeMax - fileSizeMin + 1));

        long generatedSize = fileSizeMin + randomSize;

        if(shrinkOffset != 0 && shrinkTimes != 0) {
            if(fileSizeMin + randomSize < shrinkOffset) {
                generatedSize = fileSizeMin + randomSize / shrinkTimes;
            }
        }

        return generatedSize;
    }
}
