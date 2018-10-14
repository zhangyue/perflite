/**
 * Created by zhangyue182 on 2018/07/25
 */
package ffzy.performance.client;

import ffzy.performance.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Base class of client helpers that are authenticated for cloud services.
 */
public class CloudServiceClientHelper {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    protected boolean verbose = true;

    protected String endpoint;
    protected String region;
    protected String pin = "";
    protected String userId = "";
    protected String accessKeyId = "";
    protected String secretAccessKey = "";

    public CloudServiceClientHelper(String endpoint) {
        this.endpoint = endpoint;
    }

    public CloudServiceClientHelper(String endpoint, String pin) {
        this.endpoint = endpoint;
        this.pin = pin;
    }

    public CloudServiceClientHelper(
            String endpoint,
            String region,
            UserCredential userCredential
    ) {
        this.endpoint = endpoint;
        this.region = region;
        this.pin = userCredential.getPin();
        this.userId = userCredential.getUserId();
        this.accessKeyId = userCredential.getAccessKey();
        this.secretAccessKey = userCredential.getSecretKey();
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getPin() {
        return pin;
    }

    public String getUserId() {
        return userId;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    protected void printOperation(String operation) {
        printOperation(operation, getEndpoint(), getPin(), getUserId(), getAccessKeyId(), getRegion());
    }

    protected void printOperation(String operation, String bucket) {
        printOperation(operation, getEndpoint(), getPin(), getUserId(), getAccessKeyId(), getRegion(), bucket);
    }

    protected void printOperation(String operation, String bucket, String key) {
        printOperation(operation, getEndpoint(), getPin(), getUserId(), getAccessKeyId(), getRegion(), bucket, key);
    }

    protected void printOperation(String operation, String bucket, String key, String fileNames) {
        printOperation(operation, getEndpoint(), getPin(), getUserId(), getAccessKeyId(), getRegion(), bucket, key, fileNames);
    }

    protected void printOperation(String operation, String bucket, String key, File file) {
        printOperation(operation, getEndpoint(), getPin(), getUserId(), getAccessKeyId(), getRegion(), bucket, key, file);
    }

    protected void printOperation(
            String operation, String endpoint, String pin, String userId, String accessKey, String region
    ) {
        logger.info("# [{}] {}", operation, endpoint);
        if(isVerbose()) {
            logger.info("# {} | {} | {} | {}", pin, userId, accessKey, region);
        }
    }

    protected void printOperation(
            String operation, String endpoint, String pin, String userId, String accessKey, String region,
            String bucket
    ) {
        logger.info("# [{}] {} | {}", operation, endpoint, bucket);
        if (isVerbose()) {
            logger.info("# {} | {} | {} | {}", pin, userId, accessKey, region);
        }
    }

    protected void printOperation(
            String operation, String endpoint, String pin, String userId, String accessKey, String region,
            String bucket, String key
    ) {
        logger.info("# [{}] {} | {} | {}", operation, endpoint, bucket, key);
        if(isVerbose()) {
            logger.info("# {} | {} | {} | {}", pin, userId, accessKey, region);
        }
    }

    protected void printOperation(
            String operation, String endpoint, String pin, String userId, String accessKey, String region,
            String bucket, String key, String fileNames
    ) {
        logger.info("# [{}] {} | {} | {} | {}", operation, endpoint, bucket, key, fileNames);
        if(isVerbose()) {
            logger.info("# {} | {} | {} | {}", pin, userId, accessKey, region);
        }
    }

    protected void printOperation(
            String operation, String endpoint, String pin, String userId, String accessKey, String region,
            String bucket, String key, File file
    ) {
        logger.info("# [{}] {} | {} | {} | {}", operation, endpoint, bucket, key, file);
        if(isVerbose()) {
            logger.info("# {} | {} | {} | {}", pin, userId, accessKey, region);
        }
    }
}
