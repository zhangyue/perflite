package pers.yue.common.util;

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

    public static byte[] convertStreamToByteArray(InputStream is) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] buff = new byte[100];
        int rc;
        while ((rc = is.read(buff, 0, 100)) > 0) {
            byteArrayOutputStream.write(buff, 0, rc);
        }
        return byteArrayOutputStream.toByteArray();
    }

    public static byte[] convertStreamToByteArray(InputStream is, long skip, long len) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        long buffSize = 100;
        byte[] buff = new byte[100];
        int rc;

        is.skip(skip);
        long left = len;
        buffSize = (buffSize < left) ? buffSize : left;
        while ((left > 0) && (rc = is.read(buff, 0, (int)buffSize)) > 0) {
            byteArrayOutputStream.write(buff, 0, rc);
            left = left - rc;
            buffSize = (buffSize < left) ? buffSize : left;
        }
        return byteArrayOutputStream.toByteArray();
    }

    public static String convertStreamToString(InputStream is) throws IOException {
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
    public static long consumeStream(InputStream is, byte[] buff) throws IOException {
        int numBytesRead = 0;

        int rc;
        while ((rc = is.read(buff, 0, buff.length)) > 0) {
            numBytesRead += rc;
        }

        return numBytesRead;
    }
}
