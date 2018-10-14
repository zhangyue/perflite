package ffzy.performance.util;

import com.amazonaws.util.Md5Utils;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Zhang Yue on 3/20/2018
 */
public class Md5Util {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public static String getMd5(byte[] byteArray) {
        logger.info("Calculate MD5 for byte array:");
        String hexMd5 = new String(Hex.encodeHex(Md5Utils.computeMD5Hash(byteArray)));
        logger.info("    {}", hexMd5);
        return hexMd5;
    }

    public static String getMd5(String str) {
        String preview = str;
        if(str.length() > 256) {
            preview = str.substring(0, 256);
        }
        logger.info("Calculate MD5 for string: {}", preview);
        String hexMd5 = new String(Hex.encodeHex(Md5Utils.computeMD5Hash(str.getBytes())));
        logger.info("    {}", hexMd5);
        return hexMd5;
    }

    public static String getMd5(InputStream inputStream) {
        logger.info("Calculate MD5 for input stream:");
        try {
            String hexMd5 = new String(Hex.encodeHex(Md5Utils.computeMD5Hash(inputStream)));
            logger.info("    {}", hexMd5);
            return hexMd5;
        } catch(IOException e) {
            logger.error("{} when calculate MD5 for input stream.", e.getClass().getSimpleName(), e);
            throw new RuntimeException(e);
        }
    }

    public static String getMd5(File file) {
        logger.info("Calculate MD5 for file {}:", file);
        try {
            String hexMd5 = new String(Hex.encodeHex(Md5Utils.computeMD5Hash(file)));
            logger.info("    {}", hexMd5);
            return hexMd5;
        } catch(IOException e) {
            logger.error("{} when calculate MD5 for file {}.", e.getClass().getSimpleName(), file, e);
            throw new RuntimeException(e);
        }
    }

    public static String getMd5(File file, Long start, Long end) {
        logger.info("Calculate MD5 for file {} from range {} to {}:", file, start, end);
        return getMd5(FileUtil.copyFileRange(file, start, end));
    }
}
