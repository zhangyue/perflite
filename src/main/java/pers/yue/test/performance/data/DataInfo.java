package pers.yue.test.performance.data;

import pers.yue.test.util.FileTestUtil;

import java.io.File;

/**
 * This class describes a single test data.
 *
 * Created by zhangyue182 on 2018/08/22
 */
public class DataInfo {
    private File sourceFile;
    private byte[] buffer = null;
    private String md5;

    DataInfo(File sourceFile, String md5) {
        this.sourceFile = sourceFile;
        this.md5 = md5;
    }

    public File getSourceFile() {
        return sourceFile;
    }

    public byte[] getBuffer() {
        if(buffer == null) {
            buffer = FileTestUtil.readFromFile(sourceFile);
        }

        return buffer;
    }

    public String getMd5() {
        return md5;
    }
}
