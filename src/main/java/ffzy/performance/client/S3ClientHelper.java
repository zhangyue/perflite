/**
 * Created by zhangyue182 on 2017/11/21
 */

package ffzy.performance.client;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.*;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import ffzy.performance.Launcher;
import ffzy.performance.runner.PerfRunner;
import ffzy.performance.runner.s3.DeleteObjectRunner;
import ffzy.performance.util.LogUtil;
import ffzy.performance.util.OssUtil;
import ffzy.performance.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static ffzy.performance.util.FileUtil.ByteUnit.GB;
import static ffzy.performance.util.FileUtil.ByteUnit.MB;

/**
 * S3 API client helper.
 */
public class S3ClientHelper extends CloudServiceClientHelper {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    private AmazonS3 s3Client;
    private int socketTimeout;
    private int connectionTimeout;
    private int maxErrorRetry;
    private boolean pathStyleEnabled;
    private Protocol protocol = Protocol.HTTP;

    private String lastBucketPolicyText = "";

    protected void init(final AWSCredentials awsCredentials) {
        AwsClientBuilder.EndpointConfiguration endpointConfig =
                new AwsClientBuilder.EndpointConfiguration(endpoint, region);

        ClientConfiguration config = new ClientConfiguration();
        config.setProtocol(protocol);
        config.setMaxErrorRetry(maxErrorRetry);
        config.setSocketTimeout(socketTimeout);
        config.setConnectionTimeout(connectionTimeout);
        config.setMaxConnections(2000);

        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);

        AmazonS3ClientBuilder builder = AmazonS3Client.builder()
                .withEndpointConfiguration(endpointConfig)
                .withClientConfiguration(config)
                .withCredentials(awsCredentialsProvider)
                .withPathStyleAccessEnabled(pathStyleEnabled)
                ;

        if(awsCredentials instanceof AnonymousAWSCredentials) {
            builder.withPathStyleAccessEnabled(true);
        }

        /*
        Zhang Yue (20171121): This is awkward - without this option (disableChunkedEncoding),
        client does calculation in base 64 vs. hex, never succeeds of course.
         */
        builder.disableChunkedEncoding();

        s3Client = builder.build();
    }

    private void init() {
        final AWSCredentials awsCredentials = new BasicAWSCredentials(getAccessKeyId(), getSecretAccessKey());

        init(awsCredentials);
    }

    public S3ClientHelper(
            String endpoint, final UserCredential userCredential,
            final int socketTimeout, final int connectionTimeout,
            final int maxErrorRetry, final boolean pathStyleEnabled, final Protocol protocol,
            final String region
    ) {
        super(endpoint, region, userCredential);

        this.socketTimeout = socketTimeout;
        this.connectionTimeout = connectionTimeout;
        this.maxErrorRetry = maxErrorRetry;
        this.pathStyleEnabled = pathStyleEnabled;
        this.protocol = protocol;

        init();
    }

    public AmazonS3 getS3Client() {
        return s3Client;
    }

    protected void assertExpectedError(RuntimeException e) {
        logger.error("Exception: {}", e.getMessage(), e);
    }
    public List<Bucket> getService() {
        printOperation("GetService");

        List<Bucket> bucketList = bucketList = s3Client.listBuckets();

        logger.info("# Buckets:");
        OssUtil.printService(endpoint, bucketList);
        return bucketList;
    }

    public Bucket createBucket(String bucketName) {
        return createBucket(new CreateBucketRequest(bucketName));
    }

    public Bucket createBucket(CreateBucketRequest request) {
        printOperation("CreateBucket", request.getBucketName());

        Bucket bucket = null;
        try {
            bucket = s3Client.createBucket(request);
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }

        return bucket;
    }


    public HeadBucketResult headBucket(String bucketName) {
        printOperation("HeadBucket", bucketName);

        HeadBucketResult result = null;
        try {
            result = s3Client.headBucket(new HeadBucketRequest(bucketName));
            LogUtil.printResult("Bucket Region", result.getBucketRegion());
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }
        return result;
    }

    public ListObjectsV2Result listObjectsV2(String bucketName) {
        ListObjectsV2Request request = new ListObjectsV2Request();
        request.setBucketName(bucketName);
        return getBucketV2(request);
    }

    public ListObjectsV2Result listObjectsV2(ListObjectsV2Request request) {
        return getBucketV2(request);
    }

    public ListObjectsV2Result getBucketV2(String bucketName) {
        ListObjectsV2Request request = new ListObjectsV2Request();
        return getBucketV2(request.withBucketName(bucketName));
    }

    public ListObjectsV2Result getBucketV2(ListObjectsV2Request request) {
        printOperation("GetBucketV2", request.getBucketName());
        logger.info("# Prefix: {} | Delimiter: {} | StartAfter: {} | MaxKeys: {} | ContinuationToken: {} " +
                        "| EncodingType: {} | FetchOwner: {}",
                request.getPrefix(), request.getDelimiter(), request.getStartAfter(), request.getMaxKeys(),
                request.getContinuationToken(), request.getEncodingType(), request.isFetchOwner());

        ListObjectsV2Result result = new ListObjectsV2Result();
        try {
            result = s3Client.listObjectsV2(request);

            logger.info("= Prefix: {} | Delimiter: {} | StartAfter: {} | MaxKeys: {} | ContinuationToken: {} " +
                            "| EncodingType: {} | IsTruncated: {} | NextContinuationToken: {} | KeyCount: {}",
                    result.getPrefix(), result.getDelimiter(), result.getStartAfter(), result.getMaxKeys(),
                    result.getContinuationToken(), result.getEncodingType(), result.isTruncated(),
                    result.getNextContinuationToken(), result.getKeyCount());

            List<S3ObjectSummary> s3ObjectSummary = result.getObjectSummaries();

            logger.info("= {} objects in bucket {}.", s3ObjectSummary.size(), request.getBucketName());
            OssUtil.printObjectSummary(s3ObjectSummary);
            if(result.getCommonPrefixes().size() > 0) {
                logger.info("= {} common prefixes.", result.getCommonPrefixes().size());
                LogUtil.printResult("  Common Prefixes", result.getCommonPrefixes());
            }

        } catch (RuntimeException e) {
            assertExpectedError(e);
        }

        return result;
    }

    public List<S3ObjectSummary> listAllObjectsV2(String bucketName) {
        ListObjectsV2Request request = new ListObjectsV2Request();
        return listAllObjectsV2(request.withBucketName(bucketName));
    }

    public List<S3ObjectSummary> listAllObjectsV2(ListObjectsV2Request request) {
        logger.info("List all objects in bucket {}.", request.getBucketName());

        List<S3ObjectSummary> s3ObjectSummaries = new ArrayList<>();
        ListObjectsV2Result result;

        do {
            result = getBucketV2(request);

            s3ObjectSummaries.addAll(result.getObjectSummaries());

            LogUtil.printResult("Next Continuation Token", result.getNextContinuationToken());
            request.setContinuationToken(result.getNextContinuationToken());
        } while(result.isTruncated() == true );

        logger.info("= Number of objects: {}", s3ObjectSummaries.size());

        return s3ObjectSummaries;
    }

    public boolean doesBucketExistV2(String bucketName) {
        printOperation("DoesBucketExistV2", bucketName);

        boolean doesBucketExist = false;
        try {
            doesBucketExist = s3Client.doesBucketExistV2(bucketName);
            LogUtil.printResult("Does Bucket Exist", String.valueOf(doesBucketExist));
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }

        return doesBucketExist;
    }

    public void deleteBucket(String bucketName) {
        printOperation("DeleteBucket", bucketName);

        try {
            s3Client.deleteBucket(bucketName);
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }
    }

    public void setBucketAcl(String bucketName, CannedAccessControlList cannedAcl) {
        printOperation("SetBucketAcl", bucketName);
        LogUtil.printProperty("Canned ACL", cannedAcl.toString());

        try {
            s3Client.setBucketAcl(bucketName, cannedAcl);
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }
    }

    public AccessControlList getBucketAcl(String bucketName) {
        printOperation("GetBucketAcl", bucketName);

        AccessControlList acl = new AccessControlList();
        try {
            acl = s3Client.getBucketAcl(bucketName);
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }
        return acl;
    }

    public String getBucketLocation(String bucketName) {
        printOperation("GetBucketLocation", bucketName);

        String bucketLocation = "";
        try {
            bucketLocation = s3Client.getBucketLocation(new GetBucketLocationRequest(bucketName));
            LogUtil.printResult("BucketLocation", bucketLocation);
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }
        return bucketLocation;
    }

    public PutObjectResult putObject(String bucketName, String key, String content) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.length());
        PutObjectRequest request = new PutObjectRequest(
                bucketName,
                key,
                new ByteArrayInputStream(content.getBytes()),
                metadata
        );
        return putObject(request);
    }

    public PutObjectResult putObject(PutObjectRequest request) {
        printOperation("PutObject", request.getBucketName(), request.getKey());
        if(request.getStorageClass() != null || request.getRedirectLocation() != null) {
            logger.info("# StorageClass: {} | RedirectLocation: {}", request.getStorageClass(), request.getRedirectLocation());
        }
        if(request.getTagging() != null) {
            LogUtil.printObjectTags("TagSet", request.getTagging().getTagSet());
        }
        if(request.getMetadata() != null) {
            ObjectMetadata om = request.getMetadata();
            if(om.getContentLength() != 0 || om.getContentType() != null || om.getContentMD5() != null
                    || om.getContentEncoding() != null || om.getContentDisposition() != null
                    || om.getContentLanguage() != null || om.getContentRange() != null) {
                logger.info("# ContentLength: {} | ContentType: {} | ContentMD5: {} | ContentEncoding: {} " +
                                "| ContentDisposition: {} | ContentLanguage: {} | ContentRange: {}",
                        om.getContentLength(), om.getContentType(), om.getContentMD5(), om.getContentEncoding(),
                        om.getContentDisposition(), om.getContentLanguage(), om.getContentRange());
            }
            if(om.getETag() != null || om.getCacheControl() != null || om.getLastModified() != null
                    || om.getStorageClass() != null || om.getHttpExpiresDate() != null) {
                logger.info("# Etag: {} | CacheControl: {} | LastModified: {} | StorageClass: {} " +
                                "| HttpExpiresDate: {}",
                        om.getETag(), om.getCacheControl(), om.getLastModified(), om.getStorageClass(),
                        om.getHttpExpiresDate());
            }
            if(om.getVersionId() != null || om.getPartCount() != null
                    || om.getExpirationTimeRuleId() != null || om.getOngoingRestore() != null
                    || om.getReplicationStatus() != null || om.getRestoreExpirationTime() != null ) {
                logger.info("# VersionId: {} | PartCount: {} | ExpirationTimeRuleId: {} " +
                                "| InstanceLength: {} | OngoingRestore: {} | ReplicationStatus: {} " +
                                "| RestoreExpirationTime: {}",
                        om.getVersionId(), om.getPartCount(), om.getExpirationTimeRuleId(),
                        om.getInstanceLength(), om.getOngoingRestore(), om.getReplicationStatus(),
                        om.getRestoreExpirationTime());
            }
            if(om.getSSEAlgorithm() != null || om.getSSEAwsKmsKeyId() != null || om.getSSECustomerKeyMd5() != null) {
                logger.info("# SSEAlgorithm: {} | SSEAwsKmsKeyId: {} | SSECustomerKeyMd5: {}",
                om.getSSEAlgorithm(), om.getSSEAwsKmsKeyId(), om.getSSECustomerKeyMd5());
            }
        }

        PutObjectResult putObjectResult = null;
        try {
            putObjectResult = s3Client.putObject(request);
            OssUtil.printObjectSummary(putObjectResult, isVerbose());
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }
        return putObjectResult;
    }

    public PutObjectResult putObject(String bucketName, String key, File file) {
        printOperation("PutObject", bucketName, key, file);

        PutObjectResult putObjectResult = null;
        try {
            putObjectResult = s3Client.putObject(bucketName, key, file);
            OssUtil.printObjectSummary(putObjectResult, isVerbose());
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }
        return putObjectResult;
    }

    public S3Object getObject(String bucketName, String key) {
        return getObject(new GetObjectRequest(bucketName, key));
    }

    public S3Object getObject(GetObjectRequest request) {
        printOperation("GetObject", request.getBucketName(), request.getKey());
        if(request.getRange() != null && request.getRange().length > 0) {
            if(request.getRange().length > 1) {
                LogUtil.printProperty("Range ", request.getRange()[0] + " - " + request.getRange()[1]);
            } else {
                LogUtil.printProperty("Range", request.getRange()[0]);
            }
        }
        LogUtil.printIfNotNull("# VersionId: {} | PartNumber: {}", request.getVersionId(), request.getPartNumber());
        LogUtil.printIfNotNull("# ModifiedSinceConstraint: {} | UnmodifiedSinceConstraint: {}",
                request.getModifiedSinceConstraint(), request.getUnmodifiedSinceConstraint());
        LogUtil.printIfNotNull("# MatchingETagConstraints: {} | NonmatchingETagConstraints: {}",
                request.getMatchingETagConstraints(), request.getNonmatchingETagConstraints());
        if(request.getResponseHeaders() != null) {
            ResponseHeaderOverrides rho = request.getResponseHeaders();
            LogUtil.printIfNotNull("# RHOCacheControl: {} | RHOContentDisposition: {} | RHOContentEncoding: {}",
                    rho.getCacheControl(), rho.getContentDisposition(), rho.getContentEncoding());
            LogUtil.printIfNotNull("# RHOContentType: {} | RHOContentLanguage: {} | RHOExpires: {}",
                    rho.getContentType(), rho.getContentLanguage(), rho.getExpires());
        }

        S3Object s3Object = null;
        try {
            s3Object = s3Client.getObject(request);
            OssUtil.printObjectSummary(s3Object, isVerbose());
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }

        return s3Object;
    }

    public ObjectMetadata getObjectMetadata(String bucketName, String key) {
        printOperation("GetObjectMetadata", bucketName, key);

        ObjectMetadata objectMetadata = null;
        try {
            GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(bucketName, key);
            objectMetadata = s3Client.getObjectMetadata(getObjectMetadataRequest);
            OssUtil.printObjectMetadata(objectMetadata, isVerbose());
        } catch(RuntimeException e) {
            assertExpectedError(e);
        }

        return objectMetadata;
    }

    public boolean doesObjectExist(String bucketName, String key) {
        printOperation("DoesObjectExist", bucketName, key);

        boolean doesObjectExist = false;
        try {
            doesObjectExist = s3Client.doesObjectExist(bucketName, key);
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }

        logger.info("= Does Object Exist: {}", doesObjectExist);
        return doesObjectExist;
    }

    public CopyObjectResult copyObject(
            String sourceBucketName,
            String sourceKey,
            String destinationBucketName,
            String destinationKey) {
        return copyObject(
                new CopyObjectRequest(sourceBucketName, sourceKey, destinationBucketName, destinationKey)
        );
    }

    public CopyObjectResult copyObject(CopyObjectRequest request) {
        printOperation("CopyObject", request.getDestinationBucketName(), request.getDestinationKey());
        logger.info("# Source bucket name: {} | Source key: {}", request.getSourceBucketName(), request.getSourceKey());
        if(request.getModifiedSinceConstraint() != null || request.getUnmodifiedSinceConstraint() != null) {
            logger.info("# ModifiedSinceConstraint: {} | UnmodifiedSinceConstraint: {}",
                    request.getModifiedSinceConstraint(), request.getUnmodifiedSinceConstraint());
        }

        CopyObjectResult result = null;

        try{
            result = s3Client.copyObject(request);
            if(result != null) {
                logger.info("= Etag: {} | LastModifiedDate: {} | ExpirationTime: {} | ExpirationTimeRuleId: {} | VersionId: {}",
                        result.getETag(), result.getLastModifiedDate(), result.getExpirationTime(),
                        result.getExpirationTimeRuleId(), result.getVersionId());
            }
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }

        return result;
    }

    public void deleteAllObjects(String bucketName) {
        deleteAllObjects(bucketName, 1);
    }

    public void deleteAllObjects(String bucketName, int numThread) {
        logger.info("Delete all objects of bucket {}.", bucketName);

        List<S3ObjectSummary> summaries;
        do {
            ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(1000);
            request.setEncodingType("url");
            ListObjectsV2Result result = getBucketV2(request);
            summaries = result.getObjectSummaries();
            if(0 == summaries.size()) {
                return;
            }

            List<String> keys = new ArrayList<>();
            for(S3ObjectSummary summary : summaries) {
                keys.add(summary.getKey());
            }

            if(keys.size() < (numThread > 10? 10 : numThread)) {
                numThread = 1;
            }

            PerfRunner deleteObjectRunner = new DeleteObjectRunner(this, bucketName, keys);

            Launcher launcher = new Launcher(deleteObjectRunner, numThread);
            launcher.launch();
        } while(summaries.size() > 0);
    }

    public void deleteObject(String bucketName, String key) {
        deleteObject(new DeleteObjectRequest(bucketName, key));
    }

    public void deleteObject(DeleteObjectRequest request) {
        printOperation("DeleteObject", request.getBucketName(), request.getKey());

        try {
            s3Client.deleteObject(request);
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }
    }

    private List<String> keyVersionsToKeyStrings(List<DeleteObjectsRequest.KeyVersion> keyVersions) {
        List<String> keyStrings = new ArrayList<>();
        for(DeleteObjectsRequest.KeyVersion keyVersion : keyVersions /* TODO */) {
            keyStrings.add(keyVersion.getKey());
        }
        return keyStrings;
    }

    public DeleteObjectsResult deleteMultipleObjects(DeleteObjectsRequest request) {
        printOperation("deleteMultipleObjects", getEndpoint(), getPin(), getUserId(),getSecretAccessKey(), request.getBucketName());
        LogUtil.printProperty("Keys", keyVersionsToKeyStrings(request.getKeys()), 50);

        try {
            DeleteObjectsResult deleteObjectsResult = s3Client.deleteObjects(request);
            return deleteObjectsResult;
        } catch (MultiObjectDeleteException e) {
            throw e;
        } catch (RuntimeException e) {
            assertExpectedError(e);
            return null;
        }
    }

    public CompleteMultipartUploadResult multipartUpload(String bucketName, String key, File file) {
        return multipartUpload(bucketName, key, file, 5 * MB);
    }

    public CompleteMultipartUploadResult multipartUpload(String bucketName, String key, File file, long partSize) {
        return multipartUpload(bucketName, key, file, partSize, 10);
    }

    public CompleteMultipartUploadResult multipartUpload(
            String bucketName,
            String key,
            File file,
            long partSize,
            int maxNumPartsInParallel
    ) {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, key);
        return multipartUpload(request, file, partSize, maxNumPartsInParallel);
    }

    public CompleteMultipartUploadResult multipartUpload(
            InitiateMultipartUploadRequest initRequest,
            File wholeFile,
            long partSize,
            int maxNumPartsInParallel
    ) {
        return multipartUpload(initRequest, wholeFile, partSize, maxNumPartsInParallel, 100);
    }

    public CompleteMultipartUploadResult multipartUpload(
            InitiateMultipartUploadRequest initRequest,
            File wholeFile,
            long partSize,
            int maxNumPartsInParallel,
            int intervalInMilliSeconds
    ) {
        String bucketName = initRequest.getBucketName();
        String key = initRequest.getKey();
        printOperation("MultipartUpload", bucketName, key, wholeFile);

        logger.info("# File size: {} | Part size: {} | maxNumPartsInParallel: {}",
                String.valueOf(wholeFile.length()), String.valueOf(partSize), String.valueOf(maxNumPartsInParallel));

        if(wholeFile.length() > 1 * GB) {
            logger.warn("This file is extreme large ({} GB). Uploading this file may takes hours.",
                    new DecimalFormat("#.0").format(wholeFile.length() / 1 * GB));
        }

        // Step 1: Initiate
        InitiateMultipartUploadResult initResponse = initiateMultipartUpload(initRequest);

        // Step 2: Upload
        List<PartETag> partETags = uploadParts(bucketName, key, initResponse.getUploadId(),
                wholeFile, partSize, maxNumPartsInParallel, intervalInMilliSeconds);

        // Step 3: Complete
        CompleteMultipartUploadRequest compRequest = new
                CompleteMultipartUploadRequest(bucketName, key, initResponse.getUploadId(), partETags);
        return completeMultipartUpload(compRequest);
    }

    public List<PartETag> uploadParts(
            String bucketName,
            String key,
            String uploadId,
            File wholeFile
    ) {
        return uploadParts(bucketName, key, uploadId, wholeFile, 5 * MB);
    }

    public List<PartETag> uploadParts(
            String bucketName,
            String key,
            String uploadId,
            File wholeFile,
            long partSize
    ) {
        return uploadParts(bucketName, key, uploadId, wholeFile, partSize, 10, 100);
    }

    public List<PartETag> uploadParts(
            String bucketName,
            String key,
            String uploadId,
            File wholeFile,
            long partSize,
            int maxNumPartsInParallel,
            int intervalInMilliSeconds
    ) {
        List<PartETag> partETags = new ArrayList<>();

        long contentLength = wholeFile.length();

        try {
            int numPartsInParallel = 1;
            long filePosition = 0;

            List<MultipartFileUploader> uploadersInParallel = new ArrayList<>();

            for (int i = 1; filePosition < contentLength; i++) {
                partSize = Math.min(partSize, (contentLength - filePosition)); // For last part.

                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucketName).withKey(key).withUploadId(uploadId).withPartNumber(i)
                        .withFileOffset(filePosition).withFile(wholeFile).withPartSize(partSize);

                logger.info("Upload Part {}...", i);
                MultipartFileUploader uploader = new MultipartFileUploader(uploadRequest, this);
                uploader.upload();
                uploadersInParallel.add(uploader);

                filePosition += partSize;

                if (filePosition < contentLength) {
                    ThreadUtil.sleep(intervalInMilliSeconds, TimeUnit.MILLISECONDS);
                }

                logger.info("numPartsInParallel: {}", numPartsInParallel);

                if(++numPartsInParallel <= maxNumPartsInParallel && filePosition < contentLength) {
                    continue;
                }

                logger.info("Join the parallel uploaders.");

                for (MultipartFileUploader uploaderInParallel : uploadersInParallel) {
                    uploaderInParallel.join();
                    partETags.add(uploaderInParallel.getPartETag());
                }

                logger.info("{} of {} bytes uploaded", filePosition, contentLength);

                uploadersInParallel.clear();
                numPartsInParallel = 1;
            }

           return partETags;

        } catch (InterruptedException e) {
            logger.warn("Interrupted.", e);
            logger.info("Abort Multipart Upload...");
            abortMultipartUpload(bucketName, key, uploadId);
            throw new RuntimeException(e);
        } catch (RuntimeException e) {
            logger.error("{} when upload parts.", e.getClass().getSimpleName());
            logger.info("Abort Multipart Upload...");
            abortMultipartUpload(bucketName, key, uploadId);
            throw e;
        }
    }

    public interface UploadPartRequestGenerator {
        UploadPartRequest generateUploadPartRequest();
    }

    public abstract class AbstractUploadPartRequestGenerator
            implements UploadPartRequestGenerator {
        String bucketName;
        String key;
        String uploadId;
        int partNumber;

        public AbstractUploadPartRequestGenerator(
                String bucketName,
                String key,
                String uploadId,
                int partNumber
        ) {
            this.bucketName = bucketName;
            this.key = key;
            this.uploadId = uploadId;
            this.partNumber = partNumber;
        }

        public UploadPartRequest generateUploadPartRequest() {
            return new UploadPartRequest()
                    .withBucketName(bucketName).withKey(key).withUploadId(uploadId).withPartNumber(partNumber);
        }
    }

    public class PartFileUploadPartRequestGenerator
            extends AbstractUploadPartRequestGenerator implements UploadPartRequestGenerator {
        File partFile;

        public PartFileUploadPartRequestGenerator(
                String bucketName,
                String key,
                String uploadId,
                int partNumber,
                File partFile
        ) {
            super(bucketName, key, uploadId, partNumber);

            this.partFile = partFile;
        }

        public UploadPartRequest generateUploadPartRequest() {
            return super.generateUploadPartRequest().withFile(partFile);
        }
    }

    public class WholeFileUploadPartRequestGenerator
            extends PartFileUploadPartRequestGenerator implements UploadPartRequestGenerator {
        long fileOffset;
        long partSize;

        public WholeFileUploadPartRequestGenerator(
                String bucketName,
                String key,
                String uploadId,
                int partNumber,
                File wholeFile,
                long fileOffset,
                long partSize
        ) {
            super(bucketName, key, uploadId, partNumber, wholeFile);

            this.fileOffset = fileOffset;
            this.partSize = partSize;
        }

        public UploadPartRequest generateUploadPartRequest() {
            return super.generateUploadPartRequest().withFileOffset(fileOffset).withPartSize(partSize);
        }
    }

    public class StreamUploadPartRequestGenerator
            extends AbstractUploadPartRequestGenerator implements UploadPartRequestGenerator {
        InputStream inputStream;

        public StreamUploadPartRequestGenerator(
                String bucketName,
                String key,
                String uploadId,
                int partNumber,
                InputStream inputStream
        ) {
            super(bucketName, key, uploadId, partNumber);
            this.inputStream = inputStream;
        }

        public UploadPartRequest generateUploadPartRequest() {
            return super.generateUploadPartRequest().withInputStream(inputStream);
        }
    }

    public CompleteMultipartUploadResult multipartUploadWithPartFile(
            String bucketName,
            String key,
            File partFile,
            int numParts,
            int maxNumPartsInParallel
    ) {
        File[] partFiles = new File[1];
        partFiles[0] = partFile;
        return multipartUploadWithPartFiles(bucketName, key, partFiles, numParts, maxNumPartsInParallel);
    }

    public CompleteMultipartUploadResult multipartUploadWithPartFiles(
            String bucketName,
            String key,
            File[] partFiles,
            int numParts,
            int maxNumPartsInParallel
    ) {
        String errorMessage = null;
        if(partFiles == null) {
            errorMessage = "partFiles is null.";

        }
        if(partFiles.length == 0) {
            errorMessage = "Number of part files is 0 (partFiles length is 0).";
        }
        if(errorMessage != null) {
            logger.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        int numTries = 5;

        String fileNamesStr = "";
        String partFileLengthsStr = "";
        for(File partFile : partFiles) {
            fileNamesStr += partFile + " ";
            partFileLengthsStr += partFile.length() + " ";
        }

        printOperation("MultipartUpload with part file", bucketName, key, fileNamesStr);

        logger.info("# Part file size: {} | Number of parts: {} | maxNumPartsInParallel: {}",
                partFileLengthsStr, numParts, maxNumPartsInParallel);

        List<PartETag> partETags = new ArrayList<>();

        // Step 1: Initialize.
        InitiateMultipartUploadResult initResponse = initMpu(bucketName, key);

        try {
            // Step 2: Upload parts.
            int numPartsInParallel = 0;
            int numUploadedParts = 0;

            List<MultipartFileUploader> uploadersInParallel = new ArrayList<>();

            int partFileCounter = 0;

            for (int i = 1; i <= numParts; i++) {
                File partFile = partFiles[partFileCounter++];
                if(partFileCounter == partFiles.length) {
                    partFileCounter = 0;
                }

                logger.info("partFile: {}", partFile);
                logger.info("partFile.length(): {}", partFile.length());

                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucketName).withKey(key).withUploadId(initResponse.getUploadId())
                        .withPartNumber(i).withFile(partFile).withPartSize(partFile.length());

                addPartsIntoParallelUploaders(uploadRequest, i, uploadersInParallel, numTries);

                numUploadedParts++;

                ThreadUtil.sleep(100, TimeUnit.MILLISECONDS);

                logger.info("numPartsInParallel: {}", ++numPartsInParallel);

                if(numPartsInParallel < maxNumPartsInParallel && numUploadedParts != numParts) {
                    continue;
                }

                joinParallelUploaders(uploadersInParallel, partETags, numTries);

                numPartsInParallel = 0;
            }

            if(!isAllPartsUploaded(partETags)) {
                logger.error("Not all parts got uploaded. I will not complete the multipart upload. Manual intervention required.");
                return null;
            }

            // Step 3: Complete.
            CompleteMultipartUploadRequest compRequest = new
                    CompleteMultipartUploadRequest(bucketName, key, initResponse.getUploadId(), partETags);

            logger.info("Complete Multipart Upload...");
            return completeMultipartUpload(compRequest);

        } catch (InterruptedException e) {
            logger.warn("Interrupted.", e);
            logger.info("Abort Multipart Upload...");
            abortMultipartUpload(bucketName, key, initResponse.getUploadId());
            return null;
        } catch (RuntimeException e) {
            logger.info("Abort Multipart Upload...");
            abortMultipartUpload(bucketName, key, initResponse.getUploadId());
            throw e;
        }
    }

    public CompleteMultipartUploadResult multipartUploadWithByteArrays(
            String bucketName,
            String key,
            byte[][] partByteArrays,
            int maxNumPartsInParallel
    ) {
        String errorMessage = null;
        if(partByteArrays == null) {
            errorMessage = "partByteArrays is null.";

        }
        if(partByteArrays.length == 0) {
            errorMessage = "Number of parts is 0 (partByteArrays length is 0).";
        }
        if(errorMessage != null) {
            logger.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        int numTries = 5;

        printOperation("MultipartUpload with byte arrays", bucketName, key);

        logger.info("# Number of parts: {} | maxNumPartsInParallel: {}",
                partByteArrays.length, maxNumPartsInParallel);

        List<PartETag> partETags = new ArrayList<>();

        // Step 1: Initialize.
        InitiateMultipartUploadResult initResponse = initMpu(bucketName, key);

        try {
            // Step 2: Upload parts.
            int numPartsInParallel = 0;
            int numUploadedParts = 0;

            List<MultipartFileUploader> uploadersInParallel = new ArrayList<>();

            int partFileCounter = 0;

            for (int i = 1; i <= partByteArrays.length; i++) {
                InputStream is = new ByteArrayInputStream(partByteArrays[partFileCounter++]);
                if(partFileCounter == partByteArrays.length) {
                    partFileCounter = 0;
                }

                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucketName).withKey(key).withUploadId(initResponse.getUploadId())
                        .withPartNumber(i).withInputStream(is).withPartSize(partByteArrays[i - 1].length);

                addPartsIntoParallelUploaders(uploadRequest, i, uploadersInParallel, numTries);

                numUploadedParts++;

                ThreadUtil.sleep(100, TimeUnit.MILLISECONDS);

                logger.info("numPartsInParallel: {}", ++numPartsInParallel);

                if(numPartsInParallel < maxNumPartsInParallel && numUploadedParts != partByteArrays.length) {
                    continue;
                }

                joinParallelUploaders(uploadersInParallel, partETags, numTries);

                numPartsInParallel = 0;
            }

            if(!isAllPartsUploaded(partETags)) {
                logger.error("Not all parts got uploaded. I will not complete the multipart upload. Manual intervention required.");
                return null;
            }

            // Step 3: Complete.
            CompleteMultipartUploadRequest compRequest = new
                    CompleteMultipartUploadRequest(bucketName, key, initResponse.getUploadId(), partETags);

            logger.info("Complete Multipart Upload...");
            return completeMultipartUpload(compRequest);

        } catch (InterruptedException e) {
            logger.warn("Interrupted.", e);
            logger.info("Abort Multipart Upload...");
            abortMultipartUpload(bucketName, key, initResponse.getUploadId());
            return null;
        } catch (RuntimeException e) {
            logger.info("Abort Multipart Upload...");
            abortMultipartUpload(bucketName, key, initResponse.getUploadId());
            throw e;
        }
    }

    private InitiateMultipartUploadResult initMpu(String bucketName, String key) {
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, key);
        InitiateMultipartUploadResult initResponse = initiateMultipartUpload(initRequest);
        if (initResponse == null) {
            logger.warn("Failed to initiate multipart upload.");
            throw new RuntimeException("Failed to initiate multipart upload.");
        }
        return initResponse;
    }

    private void addPartsIntoParallelUploaders(
            UploadPartRequest uploadRequest,
            int partNumber,
            List<MultipartFileUploader> uploadersInParallel,
            int numTries
    ) {
        logger.info("Upload Part {}...", partNumber);
        MultipartFileUploader uploader = new MultipartFileUploader(uploadRequest, this, numTries);
        uploader.upload();
        uploadersInParallel.add(uploader);
    }

    private void joinParallelUploaders(
            List<MultipartFileUploader> uploadersInParallel,
            List<PartETag> partETags,
            int numTries
    ) throws InterruptedException {
        logger.info("Join the parallel uploaders.");

        for (MultipartFileUploader uploaderInParallel : uploadersInParallel) {
            uploaderInParallel.join();
            partETags.add(uploaderInParallel.getPartETag());
        }

        if(partETags.size() < uploadersInParallel.size()) {
            logger.error("Only {} of {} parts uploaded with {} retries.",
                    uploadersInParallel.size() - partETags.size(), uploadersInParallel.size(), numTries);
        }

        uploadersInParallel.clear();
    }

    private boolean isAllPartsUploaded(List<PartETag>  partETags) {
        boolean allPartsUploaded = true;
        for(int i = 0; i < partETags.size(); i++) {
            if(partETags.get(i) == null || partETags.get(i).getETag() == null) {
                logger.error("Part {} is not uploaded successfully.", i + 1);
                allPartsUploaded = false;
            }
        }
        return allPartsUploaded;
    }

    private static class MultipartFileUploader extends Thread {

        private S3ClientHelper s3Helper;
        private UploadPartRequest uploadRequest;
        private PartETag partETag;
        private String eTag;
        private int numTries = 5;

        MultipartFileUploader(UploadPartRequest uploadRequest, S3ClientHelper s3Helper) {
            this.s3Helper = s3Helper;
            this.uploadRequest = uploadRequest;
        }

        MultipartFileUploader(UploadPartRequest uploadRequest, S3ClientHelper s3Helper, int numTries) {
            this.s3Helper = s3Helper;
            this.uploadRequest = uploadRequest;
            this.numTries = numTries;
        }

        @Override
        public void run() {
            UploadPartResult result = null;

            for (int i = 0; i < numTries; i++) {
                try {
                    if(i > 0) {
                        logger.info("Try {}:", i + 1);
                    }
                    result = s3Helper.uploadPart(uploadRequest);
                    break;
                } catch (RuntimeException e) {
                    logger.warn("{} when upload part, tried {} time.", e.getClass().getSimpleName(), i);
                    if(i == numTries -1) {
                        throw e;
                    }
                }
            }
            if (result == null) {
                return;
            }
            partETag = result.getPartETag();
            eTag = result.getETag();
        }

        private PartETag getPartETag() {
            return partETag;
        }

        private String getETag() {
            return eTag;
        }

        private void upload() {
            start();
        }
    }

    public CompleteMultipartUploadResult multipartCopy(
            InitiateMultipartUploadRequest initRequest,
            String sourceBucket,
            String sourceKey,
            long objectSize,
            long partSize
    ) {
        return multipartCopy(initRequest, sourceBucket, sourceKey, objectSize, partSize, 5);
    }

    public CompleteMultipartUploadResult multipartCopy(
            InitiateMultipartUploadRequest initRequest,
            String sourceBucket,
            String sourceKey,
            long objectSize,
            long partSize,
            int maxNumPartsInParallel
    ) {
        return multipartCopy(initRequest, sourceBucket, sourceKey, objectSize, partSize, maxNumPartsInParallel, 100);
    }

    public CompleteMultipartUploadResult multipartCopy(
            InitiateMultipartUploadRequest initRequest,
            String sourceBucket,
            String sourceKey,
            long objectSize,
            long partSize,
            int maxNumPartsInParallel,
            int intervalInMilliSeconds
    ) {
        String bucketName = initRequest.getBucketName();
        String key = initRequest.getKey();

        printOperation("MultipartCopy", bucketName, key);

        logger.info("# Source bucket: {} | Source key: {} | Part size: {} | maxNumPartsInParallel: {}",
                sourceBucket, sourceKey, String.valueOf(partSize), String.valueOf(maxNumPartsInParallel));

        List<PartETag> partETags = new ArrayList<>();

        // Step 1: Initialize.
        InitiateMultipartUploadResult initResponse = initiateMultipartUpload(initRequest);
        if (initResponse == null) {
            logger.warn("Failed to initiate multipart copy.");
            return null;
        }

        if(objectSize > 1 * GB) {
            logger.warn("This file is extreme large ({} GB). Uploading this file may takes hours.",
                    new DecimalFormat("#.0").format(objectSize / 1 * GB));
        }

        try {
            // Step 2: Copy parts.
            int numPartsInParallel = 1;
            long filePosition = 0;

            List<MultipartCopier> copiersInParallel = new ArrayList<>();

            for (int i = 1; filePosition < objectSize; i++) {
                partSize = Math.min(partSize, (objectSize - filePosition)); // For last part.

                CopyPartRequest copyPartRequest = new CopyPartRequest()
                        .withDestinationBucketName(bucketName).withDestinationKey(key)
                        .withSourceBucketName(sourceBucket).withSourceKey(sourceKey)
                        .withUploadId(initResponse.getUploadId()).withPartNumber(i)
                        .withFirstByte(filePosition).withLastByte(filePosition + partSize - 1);

                logger.info("Copy Part {}...", i);
                MultipartCopier copier = new MultipartCopier(copyPartRequest, this);
                copier.upload();
                copiersInParallel.add(copier);

                filePosition += partSize;

                if (filePosition < objectSize) {
                    ThreadUtil.sleep(intervalInMilliSeconds, TimeUnit.MILLISECONDS);
                }

                logger.info("numPartsInParallel: {}", numPartsInParallel);

                if(++numPartsInParallel <= maxNumPartsInParallel && filePosition < objectSize) {
                    continue;
                }

                logger.info("Join the parallel copiers.");

                for (MultipartCopier copierInParallel : copiersInParallel) {
                    copierInParallel.join();
                    partETags.add(copierInParallel.getPartETag());
                }

                copiersInParallel.clear();
                numPartsInParallel = 1;
            }

            // Step 3: Complete.
            CompleteMultipartUploadRequest compRequest = new
                    CompleteMultipartUploadRequest(bucketName, key, initResponse.getUploadId(), partETags);

            logger.info("Complete Multipart Copy...");
            return completeMultipartUpload(compRequest);
        } catch (InterruptedException e) {
            logger.warn("Interrupted.", e);
            logger.info("Abort Multipart Copy...");
            abortMultipartUpload(bucketName, key, initResponse.getUploadId());
            return null;
        } catch (RuntimeException e) {
            logger.info("Abort Multipart Copy...");
            abortMultipartUpload(bucketName, key, initResponse.getUploadId());
            throw e;
        }
    }

    private static class MultipartCopier extends Thread {

        private S3ClientHelper s3Helper;
        private CopyPartRequest copyPartRequest;
        private PartETag partETag;
        private String eTag;
        private int numTries = 5;

        MultipartCopier(CopyPartRequest copyPartRequest, S3ClientHelper s3Helper) {
            this.s3Helper = s3Helper;
            this.copyPartRequest = copyPartRequest;
        }

        MultipartCopier(CopyPartRequest copyPartRequest, S3ClientHelper s3Helper, int numTries) {
            this.s3Helper = s3Helper;
            this.copyPartRequest = copyPartRequest;
            this.numTries = numTries;
        }

        @Override
        public void run() {
            CopyPartResult result = null;

            for (int i = 0; i < numTries; i++) {
                try {
                    logger.info("Try {}:", i + 1);
                    result = s3Helper.copyPart(copyPartRequest);
                    break;
                } catch (RuntimeException e) {
                    logger.warn("{} when copy part, tried {} time.", e.getClass().getSimpleName(), i);
                    if(i == numTries -1) {
                        throw e;
                    }
                }
            }
            if (result == null) {
                return;
            }
            partETag = result.getPartETag();
            eTag = result.getETag();
        }

        private PartETag getPartETag() {
            return partETag;
        }

        private String getETag() {
            return eTag;
        }

        private void upload() {
            start();
        }
    }

    public InitiateMultipartUploadResult initiateMultipartUpload(String bucketName, String key) {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, key);
        return initiateMultipartUpload(request);
    }

    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) {
        printOperation("InitiateMultiplartUpload", request.getBucketName(), request.getKey());
        LogUtil.printProperty(request.getObjectMetadata());

        InitiateMultipartUploadResult result = null;
        try {
            result = s3Client.initiateMultipartUpload(request);
            logger.info("= UploadId: {} | AbortRuleId: {}",
                    result.getUploadId(), result.getAbortRuleId());
            if(result.getAbortDate() != null) {
                LogUtil.printResult("AbortDate", String.valueOf(result.getAbortDate()));
            }
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }
        return result;
    }

    public UploadPartResult uploadPart(UploadPartRequest request) {
        printOperation("UploadPart", request.getBucketName(), request.getKey(), request.getFile());
        logger.info("# UploadId: {} | PartNumber: {} | PartSize: {} | FileOffset: {}",
                request.getUploadId(), request.getPartNumber(), request.getPartSize(), request.getFileOffset());
        LogUtil.printProperty(request.getObjectMetadata());

        UploadPartResult result = null;
        try {
            result = s3Client.uploadPart(request);
            logger.info("= PartNumber: {} | ETag: {} | PartETag: {}",
                    result.getPartETag().getPartNumber(), result.getETag(), result.getPartETag().getETag());
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }
        return result;
    }

    public CopyPartResult copyPart(CopyPartRequest request) {
        printOperation("CopyPart", request.getDestinationBucketName());
        logger.info("# Source bucket name: {}", request.getSourceBucketName());
        logger.info("# Source Key: {}", request.getSourceKey());
        logger.info("# Destination bucket name: {}", request.getDestinationBucketName());
        logger.info("# Destination Key: {}", request.getDestinationKey());

        CopyPartResult result = null;

        try{
            result = s3Client.copyPart(request);
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }

        return result;
    }

    public CompleteMultipartUploadResult completeMultipartUpload(String bucketName, String key, String uploadId, List<PartETag> partETags) {
        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest();
        request.withBucketName(bucketName).withKey(key).withUploadId(uploadId).withPartETags(partETags);
        return completeMultipartUpload(request);
    }

    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
        printOperation("CompleteMultiplartUpload", request.getBucketName(), request.getKey());
        LogUtil.printProperty("UploadId", request.getUploadId());

        CompleteMultipartUploadResult result = null;
        try {
            result = s3Client.completeMultipartUpload(request);
            LogUtil.printIfNotNull("= VersionId: {} | ETag: {} | Location: {} | ExpirationTimeRuleId: {} " +
                            "| ExpirationTime: {}",
                    result.getVersionId(), result.getETag(), result.getLocation(), result.getExpirationTimeRuleId(),
                    result.getExpirationTime());
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }
        return result;
    }

    public void abortMultipartUpload(String bucketName, String key, String uploadId) {
        abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
    }

    public void abortMultipartUpload(AbortMultipartUploadRequest request) {
        printOperation("abortMultipartUpload", request.getBucketName(), request.getKey());
        LogUtil.printProperty("Upload ID", request.getUploadId());

        try {
            s3Client.abortMultipartUpload(
                    new AbortMultipartUploadRequest(request.getBucketName(), request.getKey(), request.getUploadId()));
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }
    }

    public MultipartUploadListing listMultipartUploads(String bucketName) {
        return listMultipartUploads(new ListMultipartUploadsRequest(bucketName));
    }

    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) {
        printOperation("ListMultiplartUpload", request.getBucketName());
        logger.info("# Prefix: {} | Delimiter: {} | MaxUploads: {} " +
                        "| KeyMarker: {} | UploadIdMarker: {} | EncodingType: {}",
                request.getPrefix(), request.getDelimiter(), request.getMaxUploads(),
                request.getKeyMarker(), request.getUploadIdMarker(), request.getEncodingType());

        MultipartUploadListing mpuListing = null;
        try {
            mpuListing = s3Client.listMultipartUploads(request);

            if(mpuListing.getPrefix() != null || mpuListing.getDelimiter() != null || mpuListing.getKeyMarker() != null
                    || mpuListing.getUploadIdMarker() != null || mpuListing.getMaxUploads() != 0 || mpuListing.getEncodingType() != null) {
                logger.info("= Prefix: {} | Delimiter: {} | MaxUploads: {} " +
                                "| KeyMarker: {} | UploadIdMarker: {} | EncodingType: {}",
                        mpuListing.getPrefix(), mpuListing.getDelimiter(), mpuListing.getMaxUploads(),
                        mpuListing.getKeyMarker(), mpuListing.getUploadIdMarker(), mpuListing.getEncodingType());
            }

            if(mpuListing.getNextKeyMarker() != null || mpuListing.getNextUploadIdMarker() != null) {
                logger.info("= NextKeyMarker: {}, NextUploadIdMarker: {}",
                        mpuListing.getNextKeyMarker(), mpuListing.getNextUploadIdMarker());
            }

            if(mpuListing.getCommonPrefixes().size() > 0) {
                LogUtil.printProperty("CommonPrefixes", mpuListing.getCommonPrefixes());
            }

            for(MultipartUpload upload : mpuListing.getMultipartUploads()) {
                logger.info("    - Key: {} | UploadId: {} | Owner: {} {}, Initiator: {} {} | Initiated: {}",
                        upload.getKey(), upload.getUploadId(),
                        upload.getOwner().getId(), upload.getOwner().getDisplayName(),
                        upload.getInitiator().getId(), upload.getInitiator().getDisplayName(),
                        upload.getInitiated());
            }
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }

        return mpuListing;
    }

    public PartListing listParts(String bucketName, String key, String uploadId) {
        ListPartsRequest listPartsRequest = new ListPartsRequest(bucketName, key, uploadId);
        return listParts(listPartsRequest);
    }

    public PartListing listParts(ListPartsRequest request) {
        printOperation("ListParts", request.getBucketName());
        logger.info("# UploadId: {} | PartNumberMarker: {} | MaxParts: {} | EncodingType: {}",
                request.getUploadId(), request.getPartNumberMarker(), request.getMaxParts(), request.getEncodingType());

        PartListing partListing = null;
        try {
            partListing = s3Client.listParts(request);
            logger.info("UploadId: {} | PartNumberMarker: {} | MaxParts: {} | EncodingType: {} " +
                            "| NextPartNumberMarker: {} | IsTruncated: {} | AbortDate: {} | AbortRuleId: {} | StorageClass: {}",
                    partListing.getUploadId(), partListing.getPartNumberMarker(), partListing.getMaxParts(),
                    partListing.getEncodingType(), partListing.getNextPartNumberMarker(), partListing.isTruncated(),
                    partListing.getAbortDate(), partListing.getAbortRuleId(), partListing.getStorageClass());
            for(PartSummary partSummary : partListing.getParts()) {
                logger.info("    - PartNumber: {} | Size: {} | ETag: {} | LastModified: {}",
                        partSummary.getPartNumber(), partSummary.getSize(), partSummary.getETag(),
                        partSummary.getLastModified());
            }
        } catch (RuntimeException e) {
            assertExpectedError(e);
        }

        return partListing;
    }
}
