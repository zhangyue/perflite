package ffzy.performance.data;

import java.io.File;

/**
 * Created by zhangyue58 on 2018/08/22
 */
public class DataInfo {
    private File sourceFile;
    private String md5;

    public DataInfo(File sourceFile, String md5) {
        this.sourceFile = sourceFile;
        this.md5 = md5;
    }

    public File getSourceFile() {
        return sourceFile;
    }

    public String getMd5() {
        return md5;
    }
}
