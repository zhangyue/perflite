package pers.yue.test.util;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.exceptions.runtime.TestRunException;
import pers.yue.common.util.Md5Util;
import pers.yue.common.util.ThreadUtil;

import java.io.File;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

/**
 * Created by Zhang Yue on 3/20/2018
 */
public class Md5TestUtil {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public static String getMd5(byte[] byteArray) {
        return Md5Util.getMd5(byteArray);
    }

    public static String getMd5(byte[] byteArray, int logLevel) {
        return Md5Util.getMd5(byteArray, logLevel);
    }

    public static String getMd5(String str) {
        return Md5Util.getMd5(str);
    }

    public static String getMd5(String str, int logLevel) {
        return Md5Util.getMd5(str, logLevel);
    }

    public static String getMd5(InputStream inputStream) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), Md5Util::getMd5, inputStream);
    }

    public static String getMd5(InputStream inputStream, int logLevel) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), Md5Util::getMd5, inputStream, logLevel);
    }

    public static String getMd5(File file) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), Md5Util::getMd5, file);
    }

    public static String getMd5(File file, int logLevel) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), Md5Util::getMd5, file, logLevel);
    }

    public static String getMd5(File file, Long start, Long end) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), Md5Util::getMd5, file, start, end);
    }

    public static String getMd5AsBase64(byte[] byteArray) {
        return Md5Util.getMd5AsBase64(byteArray);
    }

    public static String getMd5AsBase64(String str) {
        return Md5Util.getMd5AsBase64(str);
    }

    public static String getMd5AsBase64(InputStream inputStream) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), Md5Util::getMd5AsBase64, inputStream);
    }

    public static String getMd5AsBase64(File file) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), Md5Util::getMd5AsBase64, file);
    }

    public static String getMd5AsBase64(File file, Long start, Long end) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), Md5Util::getMd5AsBase64, file, start, end);
    }

    public static String getMd5AsBase64(File file, long[] range) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), Md5Util::getMd5AsBase64, file, range);
    }

    public static String hexToBase64(String hex) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), Md5Util::hexToBase64, hex);
    }

    public static byte[] decodeBase64(String base64) {
        return Md5Util.decodeBase64(base64);
    }

    public static String decodeBase64String(String base64) {
        return Md5Util.decodeBase64String(base64);
    }

    public static String md5AsBase64(byte[] input) {
        return com.amazonaws.util.Base64.encodeAsString(computeMD5Hash(input));
    }

    public static byte[] computeMD5Hash(byte[] input) {
        return Md5Util.computeMD5Hash(input);
    }

    public static String getMd5AsSafeBase64(byte[] byteArray) {
        return TestUtilBase.execute(ThreadUtil.getMethodName(), Md5Util::getMd5AsSafeBase64, byteArray);
    }

    // This is wrong. Use getMd5Aws() once the bug is fixed in JSS. See OSS-345.
    public static String getMd5Jss(List<String> md5StrList) {
        try {
            MessageDigest md5;
            md5 = MessageDigest.getInstance("MD5");
            StringBuilder totalMd5 = new StringBuilder();
            for (String md5Str : md5StrList) {
                totalMd5.append(md5Str);
            }
            byte[] buff = totalMd5.toString().getBytes(StandardCharsets.UTF_8);
            if (buff.length == 0) {
                return null;
            }
            md5.update(buff, 0, buff.length);
            String ret = new BigInteger(1, md5.digest()).toString(16);
            while (ret.length() < 32) {
                ret = "0" + ret;
            }
            return ret;
        } catch (Exception e) {
            logger.error("{} when get MD5 for MD5 list.", e.getClass().getSimpleName());
            throw new TestRunException(e);
        }
    }

    public static String getMd5Aws(List<String> md5StrList) {
        byte[] md5BytesAll = new byte[0];

        for(String md5Str : md5StrList) {
            try {
                byte[] md5Bytes = Hex.decodeHex(md5Str.toCharArray());
                byte[] lastMd5BytesAll = md5BytesAll;
                md5BytesAll = new byte[lastMd5BytesAll.length + md5Bytes.length];
                System.arraycopy(lastMd5BytesAll, 0, md5BytesAll, 0,  lastMd5BytesAll.length);
                System.arraycopy(md5Bytes, 0, md5BytesAll, lastMd5BytesAll.length, md5Bytes.length);
            } catch (DecoderException e) {
                logger.error("{} when decode hex.", e.getClass().getSimpleName(), e);
                throw new TestRunException(e);
            }
        }

        return Md5Util.getMd5(md5BytesAll);
    }
}
