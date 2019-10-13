package pers.yue.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.commons.lang.math.NumberUtils.toInt;
import static pers.yue.common.util.StringUtil.toBoolean;
import static pers.yue.common.util.StringUtil.toLong;

/**
 * Created by zhangyue58 on 2018/01/10
 */
public class PropertiesUtil {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public static String loadProperty(
            String propertyName, Properties properties, Properties commonProperties, String originalValue) {
        String propertyValue;
        if(isPropertyDefined(propertyName, properties)) {
            propertyValue = (String)properties.get(propertyName);
        } else if(isPropertyDefined(propertyName, commonProperties)) {
            propertyValue = (String)commonProperties.get(propertyName);
        } else {
            propertyValue = originalValue;
        }

        if(propertyValue == null || propertyValue.matches("<.*>")) {
            propertyValue = "";
        }

        logger.info("    {}: {}", propertyName, propertyValue);

        return propertyValue;
    }

    public static Integer loadProperty(
            String propertyName, Properties properties, Properties commonProperties, Integer originalValue) {
        String propertyString = loadProperty(propertyName, properties, commonProperties, String.valueOf(originalValue));
        try {
            return toInt(propertyString);
        } catch (NumberFormatException e) {
            logger.error("{} when parse property value {}.", e.getClass().getSimpleName(), propertyString, e);
            throw e;
        }
    }

    public static Boolean loadProperty(
            String propertyName, Properties properties, Properties commonProperties, Boolean originalValue) {
        String propertyString = loadProperty(propertyName, properties, commonProperties, String.valueOf(originalValue));
        try {
            return Boolean.valueOf(propertyString);
        } catch (NumberFormatException e) {
            logger.error("{} when parse property value {}.", e.getClass().getSimpleName(), propertyString, e);
            throw e;
        }
    }

    public static String parseProperty(String defaultValue, String property) {
        if (property == null || property.isEmpty()) {
            return defaultValue;
        }
        return property;
    }

    public static int parseProperty(int defaultValue, String property) {
        if (property == null || property.isEmpty()) {
            return defaultValue;
        }
        return toInt(property);
    }

    public static long parseProperty(long defaultValue, String property) {
        if (property == null || property.isEmpty()) {
            return defaultValue;
        }
        return toLong(property);
    }

    public static boolean parseProperty(boolean defaultValue, String property) {
        if (property == null || property.isEmpty()) {
            return defaultValue;
        }
        return toBoolean(property);
    }

    public static long parseTimeProperty(long defaultValue, String property) {
        if (property == null || property.isEmpty()) {
            return defaultValue;
        }
        if(property.endsWith("h") || property.endsWith("H")) {
            return toLong(property.substring(0, property.length() - 1)) * 60 * 60;
        }
        if(property.endsWith("m") || property.endsWith("M")) {
            return toLong(property.substring(0, property.length() - 1)) * 60;
        }
        if(property.endsWith("s") || property.endsWith("S")) {
            return toLong(property.substring(0, property.length() - 1));
        }
        return toLong(property);
    }

    public static long parseAlternativeSizeProperty(long defaultValue, String originalProperty, String alternativeProperty) {
        String valueStr = parseProperty(String.valueOf(defaultValue), originalProperty);
        if(alternativeProperty != null && !alternativeProperty.isEmpty()) {
            // Overwrite sizeProperty with sizePropertyOverwrite, if provided.
            valueStr = alternativeProperty;
        }
        return StringUtil.parseSizeStr(valueStr);
    }

    public static boolean isPropertyDefined(String propertyName, Properties properties) {
        if(properties == null || properties.isEmpty()) {
            return false;
        }
        String propertyValue = properties.getProperty(propertyName);
        return (propertyValue != null && !propertyValue.isEmpty());
    }

    public static boolean checkProperties(Map<String, String> actualProperties, Map<String, String> expectedProperties) {
        List<String> problemProperties = new ArrayList<>();
        for(Map.Entry<String, String> e : expectedProperties.entrySet()) {
            if(! actualProperties.get(e.getKey()).equals(e.getValue())) {
                problemProperties.add(e.getKey());
                problemProperties.add(actualProperties.get(e.getKey()));
                problemProperties.add(e.getValue());
            }
        }
        if(problemProperties.size() > 0) {
            LogUtil.printProperty("Properties differ", problemProperties);
            return false;
        }
        return true;
    }
}
