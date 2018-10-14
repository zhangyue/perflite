package ffzy.performance.runner.s3;

import ffzy.performance.client.S3ClientHelper;
import ffzy.performance.runner.AbstractPerfRunner;
import ffzy.performance.runner.PerfRunner;
import ffzy.performance.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static ffzy.performance.util.FileUtil.ByteUnit.MB;

/**
 * Created by zhangyue58 on 2017/11/29
 */
public abstract class AbstractObjectRunner extends AbstractPerfRunner implements Runnable, PerfRunner {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    protected List<String> bucketNames;
    protected S3ClientHelper s3Helper;

    protected static final ThreadLocal<byte[]> downloadBuffer = ThreadLocal.withInitial(() -> new byte[(int)MB * 1]);

    /**
     * For put, get and head object
     *
     * @param numLoop     For put object: numLoop must be greater than 0.
     *                    For get and head object:
     *                    If numLoop is greater than 0, then randomly select objects in numLoop.
     *                    If numLoop == -1, then get and verify all objects, or head all objects.
     * @param objectStore Stores key list and their MD5
     */
    public AbstractObjectRunner(S3ClientHelper s3Helper, String bucketName, int numLoop, Map<String, String> objectStore) {
        super(numLoop, objectStore);
        List<String> bucketNames = new ArrayList<>();
        bucketNames.add(bucketName);
        init(s3Helper, bucketNames);
    }

    /**
     * For get without data verification and head object.
     */

    /**
     * For get without data verification and head object.
     *
     * @param numLoop For get and head object:
     *                If numLoop is greater than 0, then randomly select objects in numLoop.
     *                If numLoop == -1, then get or head all objects.
     * @param keys    Key list
     */
    public AbstractObjectRunner(S3ClientHelper s3Helper, String bucketName, int numLoop, List<String> keys) {
        super(numLoop, keys);
        List<String> bucketNames = new ArrayList<>();
        bucketNames.add(bucketName);
        init(s3Helper, bucketNames);
    }

    /**
     * For put, get and delete object in a single loop
     *
     * @param numLoop Must be greater than 0.
     */
    public AbstractObjectRunner(S3ClientHelper s3Helper, String bucketName, int numLoop) {
        super(numLoop);
        List<String> bucketNames = new ArrayList<>();
        bucketNames.add(bucketName);
        init(s3Helper, bucketNames);
    }

    /**
     * For delete object (delete them all)
     *
     * @param keys Key list
     */
    public AbstractObjectRunner(S3ClientHelper s3Helper, String bucketName, List<String> keys) {
        super(keys);
        List<String> bucketNames = new ArrayList<>();
        bucketNames.add(bucketName);
        init(s3Helper, bucketNames);
    }

    protected void init(S3ClientHelper s3Helper, List<String> bucketNames) {
        this.s3Helper = s3Helper;
        this.bucketNames = bucketNames;
    }

    protected String selectBucket() {
        Random r = new Random(System.currentTimeMillis());
        return bucketNames.get(r.nextInt(bucketNames.size()));
    }
}
