package pers.yue.performance.data;

/**
 * Created by zhangyue58 on 2018/08/22
 */
public interface DataGenerator {
    void setShrinkOffset(long shrinkOffset);
    void setShrinkTimes(int shrinkTimes);

    DataInfo getDataInfo();
}
