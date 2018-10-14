package ffzy.performance.util;

import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Date;
import java.util.List;

/**
 * Created by zhangyue58 on 2017/11/21
 */
public class OssUtil {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public static void printService(String endpoint, List<Bucket> bucketList) {
        printService(endpoint, bucketList, false);
    }

    public static void printService(String endpoint, List<Bucket> bucketList, boolean verbose) {
        if (!endpoint.isEmpty()) {
            logger.info(endpoint);
        }

        if (bucketList == null) {
            return;
        }

        for (int i = 0; i < bucketList.size(); i++) {
            if (i < bucketList.size() - 1) {
                logger.info("  |- {}", bucketList.get(i).getName());
                if (verbose) {
                    logger.info("  |   |- {}", bucketList.get(i).getOwner());
                    logger.info("  |   '- {}", bucketList.get(i).getCreationDate());
                }
            } else {
                logger.info("  '- {}", bucketList.get(i).getName());
                if (verbose) {
                    logger.info("      |- {}", bucketList.get(i).getOwner());
                    logger.info("      '- {}", bucketList.get(i).getCreationDate());
                }
            }
        }
    }

    public static void printObjectSummary(List<S3ObjectSummary> objectSummaries) {
        logger.info("= Keys: ");
        for (S3ObjectSummary objectSummary : objectSummaries) {
            String displayName = "";
            String id = "";
            if (objectSummary.getOwner() != null) {
                displayName = objectSummary.getOwner().getDisplayName();
                id = objectSummary.getOwner().getId();
            }
            logger.info("    {} - Owner: { DisplayName: {}, ID: {} }, Size: {}, LastModified: {}",
                    objectSummary.getKey(),
                    displayName, id,
                    objectSummary.getSize(),
                    objectSummary.getLastModified());
        }
    }

    public static void printObjectSummary(PutObjectResult putObjectResult, boolean isVerbose) {
        if (null == putObjectResult) {
            return;
        }

        printResultIfVerbose("ContentMd5", putObjectResult.getContentMd5(), isVerbose);
        LogUtil.printResult("ETag", putObjectResult.getETag());
        printResultIfVerbose("ExpirationTime", putObjectResult.getExpirationTime(), isVerbose);
        printResultIfVerbose("ExpirationTimeRuleId", putObjectResult.getExpirationTimeRuleId(), isVerbose);
        LogUtil.printResult("VersionId", putObjectResult.getVersionId());

        printObjectMetadata(putObjectResult.getMetadata(), isVerbose);
    }

    public static void printObjectSummary(S3Object s3Object, boolean isVerbose) {
        if (null == s3Object) {
            return;
        }

        LogUtil.printResult("RedirectLocation", s3Object.getRedirectLocation());
        LogUtil.printResult("TaggingCount", s3Object.getTaggingCount());

        printObjectMetadata(s3Object.getObjectMetadata(), isVerbose);
    }

    public static void printObjectMetadata(ObjectMetadata om, boolean isVerbose) {
        if (om == null) {
            return;
        }
        printResultIfVerbose("Metadata.CacheControl", om.getCacheControl(), isVerbose);
        printResultIfVerbose("Metadata.ContentDisposition", om.getContentDisposition(), isVerbose);
        printResultIfVerbose("Metadata.ContentEncoding", om.getContentEncoding(), isVerbose);
        printResultIfVerbose("Metadata.ContentLanguage", om.getContentLanguage(), isVerbose);
        LogUtil.printResult("Metadata.ContentLength", om.getContentLength());
        printResultIfVerbose("Metadata.ContentMd5", om.getContentMD5(), isVerbose);
        if (om.getContentRange() != null) {
            LogUtil.printResult("Metadata.ContentRange.start", om.getContentRange()[0]);
            if (om.getContentRange().length > 1) {
                LogUtil.printResult("Metadata.ContentRange.end", om.getContentRange()[1]);
            }
        }
        printResultIfVerbose("Metadata.ContentType", om.getContentType(), isVerbose);
        printResultIfVerbose("Metadata.ETag", om.getETag(), isVerbose);
        printResultIfVerbose("Metadata.ExpirationTime", om.getExpirationTime(), isVerbose);
        printResultIfVerbose("Metadata.ExpirationTimeTimeRuleId", om.getExpirationTimeRuleId(), isVerbose);
        printResultIfVerbose("Metadata.HttpExpiresDate", om.getHttpExpiresDate(), isVerbose);
        printResultIfVerbose("Metadata.InstanceLength", om.getInstanceLength(), isVerbose);
        LogUtil.printResult("Metadata.LastModified", om.getLastModified());
        printResultIfVerbose("Metadata.OngoingRestore", om.getOngoingRestore(), isVerbose);
        printResultIfVerbose("Metadata.PartCount", om.getPartCount(), isVerbose);
        printResultIfVerbose("Metadata.ReplicationStatus", om.getReplicationStatus(), isVerbose);
        printResultIfVerbose("Metadata.RestoreExpirationTime", om.getRestoreExpirationTime(), isVerbose);
        printResultIfVerbose("Metadata.StorageClass", om.getStorageClass(), isVerbose);
        printResultIfVerbose("Metadata.VersionId", om.getVersionId(), isVerbose);
    }

    private static void printResultIfVerbose(String key, String value, boolean isVerbose) {
        if (isVerbose) {
            LogUtil.printResult(key, value);
        }
    }

    private static void printResultIfVerbose(String key, Integer value, boolean isVerbose) {
        if (isVerbose) {
            LogUtil.printResult(key, value);
        }
    }

    private static void printResultIfVerbose(String key, Long value, boolean isVerbose) {
        if (isVerbose) {
            LogUtil.printResult(key, value);
        }
    }

    private static void printResultIfVerbose(String key, Boolean value, boolean isVerbose) {
        if (isVerbose) {
            LogUtil.printResult(key, value);
        }
    }

    private static void printResultIfVerbose(String key, Date value, boolean isVerbose) {
        if (isVerbose) {
            LogUtil.printResult(key, value);
        }
    }

    public static String convertStreamToString(InputStream is) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuilder out = new StringBuilder();
        char[] buffer = new char[4096];
        int len = reader.read(buffer);
        while (len != -1) {
            out.append(new String(buffer, 0, len));
            len = reader.read(buffer);
        }
        return out.toString();
    }

    public static String getRegionFromEndpoint(String endpoint) {
        String region = "dummy-region";
        String s3 = "s3";
        String jcloud = "jcloudcs.com";

        String pattern = s3 + "\\..*\\." + jcloud;

        if (endpoint.matches(pattern)) {
            region = endpoint.replace(s3 + ".", "")
                    .replace("." + jcloud, "");
        }

        /*
        We no longer maintain separate bucket names for gray env as it shares database with production env.
         */

        if (region.endsWith("-stag")) {
            region = region.replace("-stag", "-1");
        }

        if (region.endsWith("-gray")) {
            region.replace("-gray", "");
        }

        return region;
    }

}
