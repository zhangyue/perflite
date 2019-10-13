package pers.yue.test.testcase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.SkipException;
import pers.yue.exceptions.runtime.TestRunException;
import pers.yue.common.util.StringUtil;
import pers.yue.common.util.ThreadUtil;

import java.util.Date;
import java.util.List;

public class TestCase {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private static final int MAX_LENGTH = 200;

    /**
     * Fails the test case and log the message and exception.
     * @param message
     * @param e
     */
    public static void fail(String message, Exception e) {
        logger.error("{}", message, e);
        logger.error(message);

        /*
        Leave the runtime exception untouched and thrown to TestNG, to feed Jenkins test report.
        */
        if(e instanceof RuntimeException) {
            throw (RuntimeException)e;
        }
        throw new TestRunException(message, e);
    }

    /**
     * Fails the test case and log the message.
     * @param message
     */
    public static void fail(String message) {
        logger.error(message);
        Assert.fail(message);
    }

    /**
     * Skip the test case, for test precondition not met.
     * @param message
     */
    public static void skip(String message, Exception e) {
        logger.error("{}", message, e);
        throw new SkipException(message);
    }

    /**
     * Skip the test case, for test precondition not met.
     * @param message
     */
    public static void skip(String message) {
        logger.error(message);
        throw new SkipException(message);
    }

    public static void isTestCaseFail(Exception e, String expectedHttpCode) {
        logger.info(e.getMessage());
        if (!e.getMessage().contains(expectedHttpCode)) {
            TestCase.fail("Unexpected exception message.");
        } else {
            logger.info("{}. Expected.", expectedHttpCode);
        }
    }

    public static void assertTrue(boolean actual){
        logger.info("(Assert) Actual: {} vs. Expected: {}", actual, true);
        if (!actual) {
            TestCase.fail("Expected true but found false.");
        }
    }

    public static void assertTrue(boolean actual, String message){
        logger.info("(Assert) Actual: {} vs. Expected: {}", actual, true);
        if (!actual) {
            TestCase.fail("Expected true but found false. " + message);
        }
    }

    public static void assertFalse(boolean actual){
        logger.info("(Assert) Actual: {} vs. Expected: {}", actual, false);
        if (actual) {
            TestCase.fail("assertTrue: actual is true.");
        }
    }

    public static void assertFalse(boolean actual, String message){
        logger.info("(Assert) Actual: {} vs. Expected: {}", actual, false);
        if (actual) {
            TestCase.fail("assertTrue: actual is true. " + message);
        }
    }

    public static void assertNotNull(Object object) {
        logger.info("(Assert) Actual: {} vs. Expected: {}", object, "Not-null");
        if (object == null){
            TestCase.fail("assertNotNull: object is null.");
        }
    }

    public static void assertNotNull(Object object, String message) {
        logger.info("(Assert) Actual: {} vs. Expected: {}", object, "Not-null");
        if (object == null){
            TestCase.fail("assertNotNull: object is null, " + message);
        }
    }

    public static void assertNull(Object object) {
        logger.info("(Assert) Actual: {} vs. Expected: {}", object, "null");
        if (object != null){
            TestCase.fail("assertNull: object is not null.");
        }
    }

    public static void assertNull(Object object, String message) {
        logger.info("(Assert) Actual: {} vs. Expected: {}", object, "null");
        if (object != null){
            TestCase.fail("assertNull: object is not null, " + message);
        }
    }

    public static void assertGT(Integer greater, Integer less) {
        logger.info("(Assert) Greater: {}; Less: {}", greater, less);
        try {
            Assert.assertTrue(greater > less);
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertGT(Integer greater, Integer less, String message) {
        logger.info("(Assert) Greater: {}; Less: {}", greater, less);
        try {
            Assert.assertTrue(greater > less, message);
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertGE(Integer greater, Integer less) {
        logger.info("(Assert) Greater: {}; Less: {}", greater, less);
        try {
            Assert.assertTrue(greater >= less);
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertGE(Integer greater, Integer less, String message) {
        logger.info("(Assert) Greater: {}; Less: {}", greater, less);
        try {
            Assert.assertTrue(greater >= less, message);
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertGT(Long greater, Long less) {
        logger.info("(Assert) Greater: {}; Less: {}", greater, less);
        try {
            Assert.assertTrue(greater > less);
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertGT(Long greater, Long less, String message) {
        logger.info("(Assert) Greater: {}; Less: {}", greater, less);
        try {
            Assert.assertTrue(greater > less, message);
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertGE(Long greater, Long less) {
        logger.info("(Assert) Greater: {}; Less: {}", greater, less);
        try {
            Assert.assertTrue(greater >= less);
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertGE(Long greater, Long less, String message) {
        logger.info("(Assert) Greater: {}; Less: {}", greater, less);
        try {
            Assert.assertTrue(greater >= less, message);
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertGT(Date later, Date earlier) {
        logger.info("(Assert) Later: {}; Earlier: {}", later, earlier);
        try {
            Assert.assertTrue(later.getTime() > earlier.getTime());
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertGT(Date later, Date earlier, String message) {
        logger.info("(Assert) Later: {}; Earlier: {}", later, earlier);
        try {
            Assert.assertTrue(later.getTime() > earlier.getTime(), message);
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertGE(Date later, Date earlier) {
        logger.info("(Assert) Later: {}; Earlier: {}", later, earlier);
        try {
            Assert.assertTrue(later.getTime() >= earlier.getTime());
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertGE(Date later, Date earlier, String message) {
        logger.info("(Assert) Later: {}; Earlier: {}", later, earlier);
        try {
            Assert.assertTrue(later.getTime() >= earlier.getTime(), message);
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertEquals(Boolean actual, Boolean expected) {
        logger.info("(Assert) Actual: {} vs. Expected: {}", actual, expected);
        try {
            Assert.assertEquals(actual, expected);
        } catch (Throwable e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertEquals(Boolean actual, Boolean expected, String message) {
        logger.info("(Assert) Actual: {} vs. Expected: {}", actual, expected);
        try {
            Assert.assertEquals(actual, expected, message);
        } catch (Throwable e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertEquals(long actual, long expected) {
        logger.info("(Assert) Actual: {} vs. Expected: {}", actual, expected);
        try {
            Assert.assertEquals(actual, expected);
        } catch (Throwable e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertEquals(long actual, long expected, String message) {
        logger.info("(Assert) Actual: {} vs. Expected: {}", actual, expected);
        try {
            Assert.assertEquals(actual, expected, message);
        } catch (Throwable e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertEquals(Date actual, Date expected) {
        logger.info("(Assert) Actual: {} vs. Expected: {}", actual, expected);
        try {
            Assert.assertEquals(actual, expected);
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertEquals(Date actual, Date expected, String message) {
        logger.info("(Assert) Actual: {} vs. Expected: {}", actual, expected);
        try {
            Assert.assertEquals(actual, expected, message);
        } catch (AssertionError e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertEquals(Date actual, Date expected, Long maxDeviationSeconds, String message) {
        logger.info("(Assert) Actual: {} vs. Expected: {} with MaxBias: {}", actual, expected);
        long actualBias = expected.getTime() - actual.getTime();
        if ( Math.abs(actualBias) > maxDeviationSeconds * 1000){
            Assert.fail(message);
            throw new AssertionError(message);
        }
    }

    public static void assertEquals(String actual, String expected) {
        logWithMaxLength(actual, expected);
        try {
            Assert.assertEquals(actual, expected);
        } catch (Throwable e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertEquals(String actual, String expected, String message) {
        logWithMaxLength(actual, expected);
        try {
            Assert.assertEquals(actual, expected, message);
        } catch (Throwable e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertEquals(byte[] actual, byte[] expected){
        logger.info("(Assert) Actual: {} vs. Expected: {}", actual, expected);
        if (expected != actual) {
            if (null == expected) {
                fail("expected a null array, but not null found.");
            }

            if (null == actual) {
                fail("expected not null array, but null found.");
            }

            assertEquals(actual.length, expected.length, "arrays don't have the same size.");

            for(int i = 0; i < expected.length; ++i) {
                if (expected[i] != actual[i]) {
                    fail("arrays differ firstly at element [" + i + "]; " + "expected value is <" + expected[i] + "> but was <" + actual[i] + ">.");
                }
            }
        }
    }

    public static void assertEquals(byte[] actual, byte[] expected, String message){
        logger.info("(Assert) Actual: {} vs. Expected: {}", actual, expected);
        if (expected != actual) {
            if (null == expected) {
                fail("expected a null array, but not null found. " + message);
            }

            if (null == actual) {
                fail("expected not null array, but null found. " + message);
            }

            assertEquals(actual.length, expected.length, "arrays don't have the same size. " + message);

            for(int i = 0; i < expected.length; ++i) {
                if (expected[i] != actual[i]) {
                    fail("arrays differ firstly at element [" + i + "]; " + "expected value is <" + expected[i] + "> but was <" + actual[i] + ">. " + message);
                }
            }
        }
    }

    /**
     * Compare two lists with strict order.
     * T needs to be implemented with "equals" method.
     * @param actual
     * @param expected
     * @param <T>
     */
    public static <T> void assertEquals(List<T> actual, List<T> expected) {
        String message = "";
        assertEquals(actual, expected, message);
    }

    /**
     * Compare two lists with strict order.
     * T needs to be implemented with "equals" method.
     * @param actual
     * @param expected
     * @param message
     * @param <T>
     */
    public static <T> void assertEquals(List<T> actual, List<T> expected, String message) {
        if(actual == null && expected == null) {
            return;
        }
        assertNullXor(actual, expected);
        assertEquals(actual.size(), expected.size());

        boolean equals = true;
        for(int i = 0; i < expected.size(); i++) {
            if(!actual.get(i).equals(expected.get(i))) {
                equals = false;
                logger.warn("Not equals: {} - {}", actual.get(i), expected.get(i));
            }
        }

        Assert.assertTrue(equals, message);
    }

    public static void assertNotEquals(String actual, String expected) {
        logWithMaxLength(actual, expected);
        try {
            Assert.assertNotEquals(actual, expected);
        } catch (Throwable e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertNotEquals(long actual, long expected, String message) {
        logger.info("(Assert) Actual: {} vs. Expected: {}", actual, expected);
        try {
            Assert.assertNotEquals(actual, expected, message);
        } catch (Throwable e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static void assertNotEquals(String actual, String expected, String message) {
        logWithMaxLength(actual, expected);
        try {
            Assert.assertNotEquals(actual, expected, message);
        } catch (Throwable e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public static <T> void assertNullXor(T actual, T expected) {
        if(actual == null && expected != null) {
            String message = "Actual is null while expected is not null.";
            logger.error(message);
            throw new TestRunException(message);
        }
        if(actual != null && expected == null) {
            String message = "Actual rule is not null while expected is null";
            logger.error(message);
            throw new TestRunException(message);
        }
    }

    public static <T> boolean isBothNull(T actual, T expected) {
        return actual == null && expected == null;
    }

    private static void logWithMaxLength(String actual, String expected) {
        logger.info("(Assert) Actual: {} vs. Expected: {}", StringUtil.purge(actual, MAX_LENGTH), StringUtil.purge(expected, MAX_LENGTH));
    }

    private static int findFirstDifferentByte(byte[] bytes1, byte[] bytes2){
        int i = 0;
        while(i < bytes1.length && i < bytes2.length){
            if (bytes1[i] != bytes2[i]) {
                break;
            }
            i++;
        }
        if (i == bytes1.length || i == bytes2.length) {
            return -1;
        } else {
            return i;
        }
    }

}
