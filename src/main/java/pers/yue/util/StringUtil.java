package pers.yue.util;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.exceptions.runtime.InvalidFormatRuntimeException;

/**
 * Created by zhangyue182 on 08/22/2018
 */
public class StringUtil {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public static String purge(String stringToPurge, int maxLength) {
        if (stringToPurge == null) {
            return null;
        }
        if (stringToPurge.length() <= maxLength) {
            return stringToPurge;
        }

        return stringToPurge.substring(0, maxLength);
    }

    public static String generateNumString(int length) {
        char[] chars = new char[10];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) (48 + i);
        }
        return generateString(chars, length);
    }

    public static String generateAlphabetString(int length) {
        char[] chars = new char[26];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) (97 + i);
        }
        return generateString(chars, length);
    }


    public static String generateAlphaNumString(int length) {
        char[] charsAlphabet = new char[26];
        for (int i = 0; i < charsAlphabet.length; i++) {
            charsAlphabet[i] = (char) (97 + i);
        }
        char[] charsNum = new char[10];
        for (int i = 0; i < charsNum.length; i++) {
            charsNum[i] = (char) (48 + i);
        }
        char[] charsAlphaNum = ArrayUtils.addAll(charsAlphabet, charsNum);
        return generateString(charsAlphaNum, length);
    }

    public static String generateChineseCharString(int length) {
        char[] chineseChars = {'零', '一', '二', '三', '四', '五', '六', '七', '八', '九'};
        return generateString(chineseChars, length);
    }

    public static String generateString(char[] chars, int length) {
        int num = 0;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(chars[num++]);
            if (num == chars.length) {
                num = 0;
            }
        }
        return sb.toString();
    }

    public static String generateRandomAlphaNumString(int length) {
        String srcString = generateAlphaNumString(36) + generateAlphabetString(26).toUpperCase();
        int len = srcString.length();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            sb.append(srcString.charAt((int) Math.round(Math.random() * (len - 1))));
        }
        return sb.toString();
    }

    public static long parseSizeStr(String sizeStr) {
        if (sizeStr.matches("\\d+GB")) {
            return StringUtil.toLong(sizeStr.replace("GB", "")) * FileUtil.GB;
        } else if (sizeStr.matches("\\d+MB")) {
            return StringUtil.toLong(sizeStr.replace("MB", "")) * FileUtil.MB;
        } else if (sizeStr.matches("\\d+KB")) {
            return StringUtil.toLong(sizeStr.replace("KB", "")) * FileUtil.KB;
        } else if (sizeStr.matches("-?\\d+")) {
            return StringUtil.toLong(sizeStr.replace("KB", ""));
        } else {
            String message = "Invalid sizeStr: " + sizeStr;
            logger.error(message);
            throw new InvalidFormatRuntimeException(message);
        }
    }

    public static int toInt(String intStr) {
        try {
            return Integer.valueOf(intStr);
        } catch (NumberFormatException e) {
            String message = "Invalid number format: " + intStr;
            logger.error(message, e);
            throw e;
        }
    }

    public static long toLong(String longStr) {
        try {
            return Long.valueOf(longStr);
        } catch (NumberFormatException e) {
            String message = "Invalid number format: " + longStr;
            logger.error(message, e);
            throw e;
        }
    }

    public static boolean toBoolean(String booleanStr) {
        try {
            return Boolean.valueOf(booleanStr);
        } catch (NumberFormatException e) {
            String message = "Invalid number format: " + booleanStr;
            logger.error(message, e);
            throw e;
        }
    }

    public static <T> String getStringFromStringOrArray(T couldBeArray, int id) {
        String myString;
        if(couldBeArray instanceof String) {
            myString = (String)couldBeArray;
        } else if(couldBeArray instanceof String[]) {
            myString = ((String[])couldBeArray)[id];
        } else {
            String message = "Unsupported type " + couldBeArray.getClass().getSimpleName();
            logger.error(message);
            throw new InvalidFormatRuntimeException(message);
        }
        return myString;
    }

    /**
     * Splits a string with the delimiter and trims all elements in the array.
     * @param line
     * @param delimiter
     * @return
     */
    public static String[] splitAndTrim(String line, String delimiter) {
        String[] words = line.split(delimiter);
        for(int i = 0; i < words.length; i++) {
            words[i] = words[i].trim();
        }
        return words;
    }

    /**
     * Trims spaces next to the delimiter characters.
     * @param source The source string.
     * @param delimiter The delimiter.
     * @return The trimmed string.
     */
    public static String trimDelimiter(String source, String delimiter) {
        return source.replaceAll("\\s+" + delimiter + "\\s+", delimiter);
    }

    /**
     * Replaces multiple continuous spaces into a single space.
     * @param source The source string.
     * @return A string that has no continuous spaces.
     */
    public static String trimRedundantWhiteSpaces(String source) {
        String delimiter = " ";
        return source.replaceAll(delimiter + "+", delimiter);
    }

    public static String getFieldValue(String source, String delimiter, int fieldPos) {
        String[] sourceArray = source.split(delimiter);
        if(sourceArray.length < fieldPos) {
            String message = "Field position " + fieldPos + " is larger than field length " + sourceArray.length + ".";
            logger.error(message);
            throw new InvalidFormatRuntimeException(message);
        }
        return sourceArray[fieldPos];
    }

    public static String getFieldValueForce(String source, String delimiter, int fieldPos) {
        String[] sourceArray = source.split(delimiter);
        if(fieldPos >= sourceArray.length) {
            return "";
        }
        return sourceArray[fieldPos];
    }

    public static String getClassSimpleName(String fullClassName) {
        int lastDelimiterPos = fullClassName.lastIndexOf(".");
        return fullClassName.substring(lastDelimiterPos + 1);
    }

    public static void main(String[] args) {
        logger.info(generateAlphabetString(30));
        logger.info(generateChineseCharString(26));
        logger.info(generateAlphaNumString(50));
        logger.info(getClassSimpleName("com.jcloud.test.ds2.Ds2SystemTest"));
    }
}
