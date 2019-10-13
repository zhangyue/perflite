package pers.yue.performance.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Created by zhangyue182 on 03/23/2018
 */
public class StreamUtil {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public static byte[] convertStreamToByteArray(InputStream is) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] buff = new byte[100];
        int rc;
        try {
            while ((rc = is.read(buff, 0, 100)) > 0) {
                byteArrayOutputStream.write(buff, 0, rc);
            }
            byte[] in2b = byteArrayOutputStream.toByteArray();
            return in2b;
        } catch (IOException e) {
            logger.error("{} when convert stream to byte array.", e.getClass().getSimpleName(), e);
            throw new RuntimeException(e);
        }
    }

    public static byte[] convertStreamToByteArray(InputStream is, long skip, long len) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        long buffSize = 100;
        byte[] buff = new byte[100];
        int rc;
        try {
            is.skip(skip);
            long left = len;
            buffSize = (buffSize < left) ? buffSize : left;
            while ((left > 0) && (rc = is.read(buff, 0, (int)buffSize)) > 0) {
                byteArrayOutputStream.write(buff, 0, rc);
                left = left - rc;
                buffSize = (buffSize < left) ? buffSize : left;
            }
            byte[] in2b = byteArrayOutputStream.toByteArray();
            return in2b;
        } catch (IOException e) {
            logger.error("{} when convert stream to byte array, skip {} length {}.",
                    e.getClass().getSimpleName(), skip, len, e);
            throw new RuntimeException(e);
        }
    }

    public static String convertStreamToString(InputStream is) {
        try (
                InputStreamReader inputStreamReader = new InputStreamReader(is);
                BufferedReader reader = new BufferedReader(inputStreamReader)
        ) {
            StringBuilder sb = new StringBuilder();
            int ch;
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            return sb.toString();
        } catch (IOException e) {
            logger.error("{} when convert stream to string.", e.getClass().getSimpleName());
            throw new RuntimeException(e);
        }
    }

    public static InputStream convertStringToInputStream(String str) {
        return new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
    }

    public static InputStream convertStringToInputStream(String str, Charset charset) {
        return new ByteArrayInputStream(str.getBytes(charset));
    }

    /**
     * Do nothing but just consume the stream.
     * @param is
     * @param buff
     * @return
     */
    public static long consumeStream(InputStream is, byte[] buff) {
        int numBytesRead = 0;

        int rc;
        try {
            while ((rc = is.read(buff, 0, buff.length)) > 0) {
                numBytesRead += rc;
            }
        } catch (IOException e) {
            logger.error("{} when convert stream to byte array.", e.getClass().getSimpleName(), e);
            throw new RuntimeException(e);
        }

        return numBytesRead;
    }
}
