package pers.yue.performance.util;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class LogUtil {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public static void logTestStep(String testStep) {
        logger.info("");
        logger.info("## STEP: {} ##", testStep);
        logger.info("");
    }

    public static void printProperty(ObjectMetadata metadata) {
        if (metadata == null) {
            return;
        }
        printProperty("Metadata - VersionId", metadata.getVersionId());
        printProperty("Metadata - ETag", metadata.getETag());
        printProperty("Metadata - ContentMd5", metadata.getContentMD5());
        printProperty("Metadata - ContentType", metadata.getContentType());
        printProperty("Metadata - ContentLength", String.valueOf(metadata.getContentLength()));
        printProperty("Metadata - ContentEncoding", metadata.getContentEncoding());
        printProperty("Metadata - ContentDisposition", metadata.getContentDisposition());
        printProperty("Metadata - LastModified", metadata.getLastModified());
        printProperty("Metadata - CacheControl", metadata.getCacheControl());
        printProperty("Metadata - PartCount", metadata.getPartCount());
        if (metadata.getContentRange() != null) {
            printProperty("Metadata.ContentRange.start",
                    String.valueOf(metadata.getContentRange()[0]));
            if (metadata.getContentRange().length > 1) {
                printProperty("Metadata.ContentRange.start",
                        String.valueOf(metadata.getContentRange()[1]));
            }
        }
    }

    public static void printProperty(String name, String value) {
        logger.info("# {}: {}", name, value);
    }

    public static void printProperty(String name, Date value) {
        logger.info("# {}: {}", name, value);
    }

    public static void printProperty(String name, List<String> values) {
        printProperty(name, values, null);
    }

    public static void printProperty(String name, List<String> values, Integer maxLengthToPrint) {
        if (values == null) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if(maxLengthToPrint != null && i > maxLengthToPrint) {
                logger.info("...");
                break;
            }
            sb.append(values.get(i)).append(", ");
        }

        if (values.size() > 0) {
            logger.info("# {}: {}", name, sb.toString().substring(0, sb.toString().length() - 2));
            logger.info("# {} items in total.", values.size());
        } else {
            logger.info("# {}: empty", name);
        }
    }


    public static void printObjectTags(String name, List<Tag> tags) {
        if (tags == null) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (Tag tag : tags) {
            sb.append(tag.getKey()).append(":").append(tag.getValue()).append(", ");
        }

        if (tags.size() > 0) {
            logger.info("# {}: {}", name, sb.toString().substring(0, sb.toString().length() - 2));
        } else {
            logger.info("# {}: empty", name);
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

    public static void printProperty(String name, Boolean value) {
        if (value == null) {
            return;
        }
        printProperty(name, String.valueOf(value));
    }

    public static void printProperty(String name, Long value) {
        if (value == null) {
            return;
        }
        printProperty(name, String.valueOf(value));
    }

    public static void printProperty(String name, Integer value) {
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

    public static void printResult(String name, String value) {
        if (value != null && !value.equals("")) {
            logger.info("= {}: {}", name, value);
        }
    }

    public static void printResult(String name, Long value) {
        if (value != null && value != 0) {
            logger.info("= {}: {}", name, value);
        }
    }

    public static void printResult(String name, Boolean value) {
        if (value != null) {
            logger.info("= {}: {}", name, value);
        }
    }

    public static void printResult(String name, Date value) {
        if (value != null) {
            logger.info("= {}: {}", name, value);
        }
    }

    public static void printResult(String name, List<String> values) {
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

    public static void printResult(Map<String, String> properties) {
        for (Map.Entry<String, String> e : properties.entrySet()) {
            printResult(e.getKey(), e.getValue());
        }
    }

    public static void printResult(String name, Integer value) {
        if (value == null) {
            return;
        }
        printResult(name, String.valueOf(value));
    }
}
