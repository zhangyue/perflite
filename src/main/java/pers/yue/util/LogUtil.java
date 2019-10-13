package pers.yue.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class LogUtil {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public static final int LOG_LEVEL_0_PERF = 0; // For performance / stress test.
    public static final int LOG_LEVEL_V_LIGHT_STRESS = 1; // For light stress test.
    public static final int LOG_LEVEL_VV_FUNCTIONAL = 2; // For functional test.
    public static final int LOG_LEVEL_VVVV = 4; // Not used yet.

    public static void logAndSOutInfo(Logger logger, String message) {
        logger.info(message);
        System.out.println(DateUtil.formatTime(DateUtil.getCurrentTime(), DateUtil.FORMAT_SHORT_COMPACT)
                + " [" + Thread.currentThread().getName() + "] INFO " + message);
    }

    public static void logAndSOutWarn(Logger logger, String message) {
        logger.warn(message);
        System.out.println(DateUtil.formatTime(DateUtil.getCurrentTime(), DateUtil.FORMAT_SHORT_COMPACT)
                + " [" + Thread.currentThread().getName() + "] WARN " + message);
    }

    public static void logAndSOutError(Logger logger, String message) {
        logger.error(message);
        System.out.println(DateUtil.formatTime(DateUtil.getCurrentTime(), DateUtil.FORMAT_SHORT_COMPACT)
                + " [" + Thread.currentThread().getName() + "] ERROR " + message);
    }

    public static void logTestStep(String testStep) {
        logger.info("");
        logger.info("## STEP: {} ##", testStep);
        logger.info("");
    }

    public static <T> void printProperty(String name, String value) {
        if(value == null || value.isEmpty()) {
            return;
        }
        logger.info("# {}: {}", name, value);
    }

    public static void printProperty(String name, Date value) {
        logger.info("# {}: {}", name, value);
    }

    public static void printProperty(String name, List<String> values) {
        printProperty(name, values, null);
    }

    public static String listToLine(List<String> values, Integer maxLengthToAppend) {
        if (values == null || values.size() == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if(maxLengthToAppend != null && i > maxLengthToAppend) {
                return sb.append(" ...").toString();
            }
            sb.append(values.get(i)).append(", ");
        }
        return sb.toString().substring(0, sb.toString().length() - 2);
    }

    public static void printProperty(String name, List<String> values, Integer maxLengthToPrint) {
        if (values == null) {
            return;
        }

        if (values.size() > 0) {
            logger.info("# {}: {}", name, listToLine(values, maxLengthToPrint));
            logger.info("# {} items in total.", values.size());
        } else {
            logger.info("# {}: empty", name);
        }
    }

    public static void printPropertiesLine(Map<String, String> propertiesLine, String lineHead) {
        printPropertiesLines(Collections.singletonList(propertiesLine), lineHead);
    }

    public static void printPropertiesLines(List<Map<String, String>> propertiesLines, String lineHead) {
        if (null == propertiesLines) {
            return;
        }
        for(Map<String, String> propertiesLine : propertiesLines) {
            if(propertiesLine == null || propertiesLine.size() == 0) {
                continue;
            }
            StringBuilder lineBuilder = new StringBuilder(lineHead).append(" ");
            for(Map.Entry<String, String> e : propertiesLine.entrySet()) {
                if(e.getValue() != null) {
                    lineBuilder.append(e.getKey()).append(": ").append(e.getValue()).append(" | ");
                } else {
                    lineBuilder.append(e.getKey()).append(" | ");
                }
            }
            String line = lineBuilder.toString();
            logger.info(line.substring(0, line.length() - 2));
        }
    }

    public static void printProperty(Map<String, String> properties) {
        if(properties == null) {
            return;
        }

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            printProperty(entry.getKey(), entry.getValue());
        }
    }

    public static <T> void printProperty(String name, T value) {
        if (value == null) {
            return;
        }
        printProperty(name, String.valueOf(value));
    }

    public static void printIfNotNull(String format, Object... arguments) {
        for (Object arg : arguments) {
            if (arg == null) {
                continue;
            } else if (arg instanceof Object[]) {
                if (((Object[]) arg).length == 0) {
                    continue;
                }
            } else if (arg instanceof List) {
                if (((List) arg).size() == 0) {
                    continue;
                }
            }
            logger.info(format, arguments);
            break;
        }
    }

    public static <T> void printResult(String name, T value) {
        printResult(name, value, LOG_LEVEL_VV_FUNCTIONAL);
    }

    public static <T> void printResult(String name, T value, int logLevel) {
        if(logLevel < LOG_LEVEL_V_LIGHT_STRESS) {
            return;
        }
        if (value != null && !value.equals("")) {
            logger.info("= {}: {}", name, value);
        }
    }

    public static void printResult(String name, List<String> values) {
        printResult(name, values, LOG_LEVEL_VV_FUNCTIONAL);
    }

    public static void printResult(String name, List<String> values, int logLevel) {
        if(logLevel < LOG_LEVEL_V_LIGHT_STRESS) {
            return;
        }

        if (values == null) {
            return;
        }

        if (values.size() > 0) {
            for (String value : values) {
                logger.info("= {}: {}", name, value);
            }
        } else {
            logger.info("= {}: empty", name);
        }
    }

    public static void printResult(String key, Map<String, String> properties, int logLevel) {
        if(properties == null || properties.size() == 0) {
            return;
        }
        StringBuilder sb = new StringBuilder("= " + key + ": [");
        for (Map.Entry<String, String> e : properties.entrySet()) {
            printResult(e.getKey(), e.getValue(), logLevel);
            sb.append(e.getKey()).append(": ").append(e.getValue()).append(" | ");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
    }

    public static void printResult(Map<String, String> properties) {
        printResult(properties, LOG_LEVEL_VV_FUNCTIONAL);
    }

    public static void printResult(Map<String, String> properties, int logLevel) {
        for (Map.Entry<String, String> e : properties.entrySet()) {
            printResult(e.getKey(), e.getValue(), logLevel);
        }
    }

    public static <T> void putIfNotNull(Map<String, String> map, String key, T value) {
        if(key == null || value == null) {
            return;
        }

        map.put(key, String.valueOf(value));
    }


}
