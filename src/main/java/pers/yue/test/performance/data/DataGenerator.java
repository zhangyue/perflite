package pers.yue.test.performance.data;

/**
 * The interface for generating test data.
 *
 * Created by zhangyue182 on 2018/08/22
 */
public interface DataGenerator {
    void setFileTag(String fileTag);
    void setShrinkOffset(long shrinkOffset);
    void setShrinkTimes(int shrinkTimes);

    DataInfo getDataInfo();
}
