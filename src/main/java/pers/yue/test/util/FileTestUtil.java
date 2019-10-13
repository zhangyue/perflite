package pers.yue.test.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.exceptions.runtime.TestRunException;
import pers.yue.util.FileUtil;
import pers.yue.util.ThreadUtil;

import java.io.*;

/**
 * Created by zhangyue182 on 2019/05/10
 */
public class FileTestUtil {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    /**
     * Create file with pretty generated content.
     * @param file
     * @param size
     * @return
     */
    public static File generateFileContent(File file, long size) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::generateFileContent, file, size);
    }

    public static File writeToFile(String content, String pathToFile) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::writeToFile, content, pathToFile);
    }

    public static File writeToFile(String content, File file) {
        return writeToFile(content, file, false);
    }

    public static File writeToFile(String content, File file, boolean append) {
        try (FileWriter writer = new FileWriter(file, append)) {
            writer.write(content);
            return file;
        } catch (IOException e) {
            logger.error("{} when {}.", e.getClass().getSimpleName(), ThreadUtil.getMethodName(), e);
            throw new TestRunException(e);
        }
    }

    public static File writeToFile(InputStream inputStream, String pathToFile) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::writeToFile, inputStream, pathToFile);
    }

    public static File writeToFile(InputStream inputStream, File file) {
        return writeToFile(inputStream, file, false);
    }

    public static File writeToFile(InputStream inputStream, File file, boolean append){
        logger.info("Write input stream to file {}.", file);

        try (FileOutputStream fos = new FileOutputStream(file, append)) {
            int read;
            byte[] bytes = new byte[1024];
            while ((read = inputStream.read(bytes)) != -1) {
                fos.write(bytes, 0, read);
            }
            return file;
        } catch (IOException e) {
            logger.error("{} when {}.", e.getClass().getSimpleName(), ThreadUtil.getMethodName(), e);
            throw new TestRunException(e);
        }
    }

    public static byte[] readFromFile(File file, long start, long end) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::readFromFile, file, start, end);
    }

    public static byte[] readFromFile(File file) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::readFromFile, file);
    }

    public static String readStringFromFile(String pathToFile) {
        return readStringFromFile(new File(pathToFile));
    }

    public static String readStringFromFile(File file) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::readStringFromFile, file);
    }

    public static String readStringFromFile(String pathToFile, long start, long end) {
        return readStringFromFile(new File(pathToFile), start, end);
    }

    public static String readStringFromFile(File file, long start, long end) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::readStringFromFile, file, start, end);
    }

    public static File copyFileRange(File sourceFile, Long start, Long end) {
        String pathToRangeFile = sourceFile.getPath() + "-" + start;
        if(end != null) {
            pathToRangeFile += "-" + end;
        }
        File rangeFile = new File(pathToRangeFile);
        copyFileRange(sourceFile, start, end, rangeFile);
        return rangeFile;
    }

    public static void copyFileRange(File sourceFile, long start, long end, File destFile) {
        try {
            FileUtil.copyFileRange(sourceFile, start, end, destFile);
        } catch (IOException e) {
            logger.error("{} when {}.", e.getClass().getSimpleName(), ThreadUtil.getMethodName(), e);
            throw new TestRunException(e);
        }
    }

    public static InputStream getFileInputStream(File file) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileInputStream::new, file);
    }

    public static File getFileFromLink(String urlString, String path) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::getFileFromLink, urlString, path);
    }

    public static String getFilePreview(File file) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::getFilePreview, file);
    }

    public static boolean contentEquals(File file1, File file2) {
        logger.info("Compare content of file {} and {}", file1, file2);
        try {
            return FileUtils.contentEquals(file1, file2);
        } catch (IOException e) {
            String message = e.getClass().getSimpleName() + " when compare two files " + file1.getPath() + " vs. " + file2.getPath();
            logger.error(message);
            throw new TestRunException(message, e);
        }
    }

    public static File createNewFileInTmp(String relativePath, String fileName) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::createNewFileInTmp, relativePath, fileName);
    }

    public static File createFile(String pathToFile) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::createFile, pathToFile);
    }

    public static boolean createNewFile(File file) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::createNewFile, file);
    }

    public static File createRandomAccessFile(String pathToFile, long size) {
        return createRandomAccessFile(new File(pathToFile), size);
    }

    public static File prepareNewFile(String pathToFile) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::prepareNewFile, pathToFile);
    }

    /**
     * Creates large file in less time than generating pretty content.
     * @param file
     * @param size
     * @return
     */
    public static File createRandomAccessFile(File file, long size) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), FileUtil::createRandomAccessFile, file, size);
    }

    public static byte[] loadIntoMemory(File file) {
        if(file.length() > Integer.MAX_VALUE) {
            throw new TestRunException("File is too large (> 2 GB).");
        }

        try {
            return IOUtils.toByteArray(new FileInputStream(file));
        } catch (IOException e) {
            String message = e.getClass().getSimpleName() + " when load file " + file.getPath();
            logger.error(message);
            throw new TestRunException(message, e);
        }
    }

    public static void main(String[] args) {
        readStringFromFile(new File("non-exist-dir/non-exist-file1"), 1, 2);
    }
}
