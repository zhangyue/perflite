package pers.yue.common.util;

import com.amazonaws.util.Md5Utils;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.exceptions.ExceedingBoundaryException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static pers.yue.common.util.LogUtil.LOG_LEVEL_VV_FUNCTIONAL;
import static pers.yue.common.util.LogUtil.LOG_LEVEL_V_LIGHT_STRESS;

/**
 * Created by Zhang Yue on 3/20/2018
 */
public class Md5Util {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public static String getMd5(byte[] byteArray) {
        return getMd5(byteArray, LOG_LEVEL_VV_FUNCTIONAL);
    }

    public static String getMd5(byte[] byteArray, int logLevel) {
        if(logLevel > LOG_LEVEL_V_LIGHT_STRESS) {
            logger.info("Calculate MD5 for byte array:");
        }
        return md5BytesToString(Md5Utils.computeMD5Hash(byteArray), logLevel);
    }

    public static String getMd5(String str) {
        return getMd5(str, LOG_LEVEL_VV_FUNCTIONAL);
    }

    public static String getMd5(String str, int logLevel) {
        String preview = str;
        if(str.length() > 256) {
            preview = str.substring(0, 256);
        }
        if(logLevel > LOG_LEVEL_V_LIGHT_STRESS) {
            logger.info("Calculate MD5 for string: {}", preview);
        }
        return md5BytesToString(Md5Utils.computeMD5Hash(str.getBytes()), logLevel);
    }

    public static String getMd5(InputStream inputStream) throws IOException {
        return getMd5(inputStream, LOG_LEVEL_VV_FUNCTIONAL);
    }

    public static String getMd5(InputStream inputStream, int logLevel) throws IOException {
        if(logLevel > LOG_LEVEL_V_LIGHT_STRESS) {
            logger.info("Calculate MD5 for input stream:");
        }
        return md5BytesToString(Md5Utils.computeMD5Hash(inputStream), logLevel);
    }

    public static String getMd5(File file) throws IOException {
        return getMd5(file, LOG_LEVEL_VV_FUNCTIONAL);
    }

    public static String getMd5(File file, int logLevel) throws IOException {
        if(logLevel > LOG_LEVEL_V_LIGHT_STRESS) {
            logger.info("Calculate MD5 for file {}:", file);
        }
        return md5BytesToString(Md5Utils.computeMD5Hash(file), logLevel);
    }

    private static String md5BytesToString(byte[] md5Bytes, int logLevel) {
        String hexMd5 = new String(Hex.encodeHex(md5Bytes));
        if(logLevel > LOG_LEVEL_V_LIGHT_STRESS) {
            logger.info("    {}", hexMd5);
        }
        return hexMd5;
    }

    public static String getMd5(File file, Long start, Long end) throws IOException {
        logger.info("Calculate MD5 for file {} from range {} to {}:", file, start, end);
        return getMd5(FileUtil.copyFileRange(file, start, end));
    }

    public static String getMd5AsBase64(byte[] byteArray) {
        byte[] byteArrayToPrint = Arrays.copyOf(byteArray, byteArray.length);
        if(byteArray.length > 100) {
            byteArrayToPrint = Arrays.copyOfRange(byteArray, 0, 99);
        }
        logger.info("Calculate MD5 as BASE64 for byte array {}:", byteArrayToPrint);
        String base64Md5 = Md5Utils.md5AsBase64(byteArray);
        logger.info("    {}", base64Md5);
        return base64Md5;
    }

    public static String getMd5AsBase64(String str) {
        logger.info("Calculate MD5 as BASE64 for string {}:", str);
        String base64Md5 = Md5Utils.md5AsBase64(str.getBytes());
        logger.info("    {}", base64Md5);
        return base64Md5;
    }

    public static String getMd5AsBase64(InputStream inputStream) throws IOException {
        logger.info("Calculate MD5 as BASE64 for input stream:");
        String base64Md5 = Md5Utils.md5AsBase64(inputStream);
        logger.info("    {}", base64Md5);
        return base64Md5;
    }

    public static String getMd5AsBase64(File file) throws IOException {
        logger.info("Calculate MD5 as BASE64 for file {}:", file);
        String base64Md5 = Md5Utils.md5AsBase64(file);
        logger.info("    {}", base64Md5);
        return base64Md5;
    }

    public static String getMd5AsBase64(File file, Long start, Long end) throws IOException {
        logger.info("Calculate MD5 as BASE64 for file {} from range {} to {}:", file, start, end);
        return getMd5AsBase64(FileUtil.copyFileRange(file, start, end));
    }

    public static String getMd5AsBase64(File file, long[] range) throws IOException, ExceedingBoundaryException {
        String md5;
        if(range == null) {
            md5 = Md5Util.getMd5AsBase64(file);
        } else {
            if(range.length == 1) {
                md5 = Md5Util.getMd5AsBase64(file, range[0], null);
            } else if(range.length == 2) {
                md5 = Md5Util.getMd5AsBase64(file, range[0], range[1]);
            } else {
                String message = "Unexpected number of ranges.";
                logger.error(message);
                throw new ExceedingBoundaryException(message);
            }
        }
        return md5;
    }

    public static String hexToBase64(String hex) throws DecoderException {
        logger.info("Convert hex string {} to base64:", hex);
        byte[] decoded = Hex.decodeHex(hex.toCharArray());
        String base64 = Base64.encodeBase64String(decoded);
        logger.info("    {}", base64);
        return base64;
    }

    public static byte[] decodeBase64(String base64) {
        logger.info("Decode base64 {} :", base64);
        byte[] decoded = Base64.decodeBase64(base64);
        logger.info("    {}", decoded);
        return decoded;
    }

    public static String decodeBase64String(String base64) {
        byte[] decoded = decodeBase64(base64);

        String decodedString = new String(decoded);
        logger.info("    {}", decodedString);
        return base64;
    }

    public static String md5AsBase64(byte[] input) {
        return com.amazonaws.util.Base64.encodeAsString(computeMD5Hash(input));
    }

    public static byte[] computeMD5Hash(byte[] input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return md.digest(input);
        } catch (NoSuchAlgorithmException e) {
            // should never get here
            throw new IllegalStateException(e);
        }
    }

    public static String getMd5AsSafeBase64(byte[] byteArray) {
        byte[] byteArrayToPrint = Arrays.copyOf(byteArray, byteArray.length);
        if(byteArray.length > 100) {
            byteArrayToPrint = Arrays.copyOfRange(byteArray, 0, 99);
        }
        logger.info("Calculate MD5 as BASE64 for byte array {}:", byteArrayToPrint);
        String safebase64Md5 =  Md5Util.md5AsBase64(byteArray).replace('+', '-').replace('/', '_');
        logger.info("{}", safebase64Md5);
        return safebase64Md5;
    }
}
