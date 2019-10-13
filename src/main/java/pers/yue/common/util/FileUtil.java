package pers.yue.common.util;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.yue.exceptions.ExceedingBoundaryException;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import static javax.ws.rs.core.MediaType.*;

/**
 * Created by Zhang Yue on 2019/10/13
 */
public class FileUtil {
    private static Logger logger = LoggerFactory.getLogger(ThreadUtil.getClassName());

    public static long KB = 1024L;
    public static long MB = 1024 * KB;
    public static long GB = 1024 * MB;
    public static long TB = 1024 * GB;
    public static long PB = 1024 * TB;
    public static long EB = 1024 * PB;

    public static final String TMP_DIRECTORY_PATH = "./tmp/";

    public class ByteUnit {
        public static final long KB = 1024L;
        public static final long MB = 1048576L;
        public static final long GB = 1073741824L;
        public static final long TB = 1099511627776L;
    }

    public static long sizeStrToLong(String sizeStr) {
        long size = -1;
        if(sizeStr.contains("KB")) {
            size = StringUtil.toLong(sizeStr.replace("KB", "")) * KB;
        } else if(sizeStr.contains("MB")) {
            size = StringUtil.toLong(sizeStr.replace("MB", "")) * MB;
        } else if(sizeStr.contains("GB")) {
            size = StringUtil.toLong(sizeStr.replace("GB", "")) * GB;
        } else if(sizeStr.contains("TB")) {
            size = StringUtil.toLong(sizeStr.replace("TB", "")) * TB;
        }
        return size;
    }

    public static File prepareNewFile(String pathToFile) throws IOException {
        File newFile = new File(pathToFile);
        if(newFile.exists()) {
            newFile.delete();
        }
        newFile.getParentFile().mkdirs();
        FileUtil.createNewFile(newFile);
        return newFile;
    }

    public static File createNewFileInTmp(String relativePath, String fileName) throws IOException {
        File newDir = new File(TMP_DIRECTORY_PATH + File.separator + relativePath);
        newDir.mkdirs();
        File newFile = new File(TMP_DIRECTORY_PATH + File.separator + relativePath, fileName);
        createNewFile(newFile);
        return newFile;
    }

    public static File[] listFilesInTmp(String relativePath) {
        File tmpDir = new File(TMP_DIRECTORY_PATH + File.separator + relativePath);
        return tmpDir.listFiles();
    }

    public static File createFile(String pathToFile) throws IOException {
        File file = new File(pathToFile);
        if(file.exists()) {
            return file;
        }
        if (file.createNewFile()) {
            return file;
        } else {
            String message = "Failed to create new file " + file.getPath();
            logger.error(message);
            throw new IOException(message);
        }
    }

    public static boolean createNewFile(File file) throws IOException {
        if (file.exists()) {
            return true;
        }
        return file.createNewFile();
    }

    /**
     * Create file with pretty generated content.
     * @param file
     * @param size
     * @return
     */
    public static File generateFileContent(File file, long size) throws IOException {
        logger.info("Generate content for file {} length {}.", file, size);

        int maxLineLength = 512;

        try (RandomAccessFile r = new RandomAccessFile(file, "rw")) {

            r.setLength(size);

            long generatedSize = 0;
            int lineNumber = 0;
            byte[] line;

            while(generatedSize < size) {
                String timeString = getTimeString();
                long remainingLong = size - generatedSize;
                line = generateLine(lineNumber++, timeString, maxLineLength, remainingLong);
                generatedSize += line.length;
                r.write(line);
            }
        }

        return file;
    }

    public static File generateLargeFile(File file, long size) throws IOException {
        logger.info("Generate large file {} length {}.", file, size);
        try (
                FileOutputStream fos = new FileOutputStream(file);
                FileChannel output = fos.getChannel()
        ) {
            output.write(ByteBuffer.allocate(1), size - 1);
        }
        return file;
    }

    public static File createRandomAccessFile(String pathToFile, long size) throws IOException {
        return createRandomAccessFile(new File(pathToFile), size);
    }

    /**
     * Creates large file in less time than generating pretty content.
     * @param file
     * @param size
     * @return
     */
    public static File createRandomAccessFile(File file, long size) throws IOException {
        logger.info("Generate binary content for file {} length {}.", file, size);

        try(RandomAccessFile r = new RandomAccessFile(file, "rw")) {
            r.setLength(size);
            return file;
        }
    }

    public static File getFileFromLink(String urlString, String path) throws IOException {
        System.gc();
        File file = new File(path);
        try (OutputStream os = new FileOutputStream(file)) {
            URL url = new URL(urlString);
            URLConnection con = url.openConnection();
            try (InputStream is = con.getInputStream()) {
                byte[] bs = new byte[1024];
                int len;
                System.gc();        // make sure file is released. // Yue @20180329: TODO Why System.gc()?
                while ((len = is.read(bs)) != -1) {
                    os.write(bs, 0, len);
                }
            }
            return file;
        }
    }

    private static String getTimeString() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss HHH");
        return simpleDateFormat.format(new Date()) + " created by OSS QE.";
    }

    private static byte[] generateLine(long lineNumber, String fixedStr, int fixedMaxLength, long remainingLength) {
        byte[] returnBytes;

//        logger.debug("Fixed max line length: {}.", fixedMaxLength);
//        logger.debug("Remaining length: {}.", remainingLength);

        if(fixedMaxLength < 1) {
            logger.warn("Max line length is less than 1. Nothing to generate.");
            return new byte[0];
        }

        int length;
        String headStr = String.format("%012d", lineNumber) + " " + fixedStr;
//        logger.debug("Head string length: {}.", headStr.length());

        if(remainingLength < headStr.length()) {
            length = (int)remainingLength;
            returnBytes = new byte[length];
            System.arraycopy(headStr.getBytes(), 0, returnBytes, 0, length - 1);
            returnBytes[length - 1] = '\n';

            return returnBytes;
        }

        if(remainingLength < fixedMaxLength) {
            length = (int)remainingLength;
        } else {
            length = headStr.length() + new Random(DateUtil.getCurrentTime())
                    .nextInt(fixedMaxLength - headStr.length() + 1);
        }

//        logger.debug("Random line length: {}.", length);

        returnBytes = new byte[length];
        System.arraycopy(headStr.getBytes(), 0, returnBytes, 0, headStr.getBytes().length);

        for (int i = headStr.getBytes().length; i < length; i++) {
            if (i == length - 1) {
                returnBytes[i] = '\n';
            } else if (i == headStr.getBytes().length) {
                returnBytes[i] = ' ';
            } else {
                returnBytes[i] = '*';
            }
        }

        return returnBytes;
    }

    public static File writeToFile(String content, String pathToFile) throws IOException {
        File file = new File(pathToFile);
        return writeToFile(content, file);
    }

    public static File writeToFile(String content, File file) throws IOException {
        return writeToFile(content, file, false);
    }

    public static File writeToFile(String content, File file, boolean append) throws IOException {
        try (FileWriter writer = new FileWriter(file, append)) {
            writer.write(content);
            return file;
        }
    }

    public static File writeToFile(InputStream inputStream, String pathToFile) throws IOException {
        File file = new File(pathToFile);
        return writeToFile(inputStream, file);
    }

    @Deprecated
    public static byte[] readFromFile_Retired(File file, int start, int end) throws IOException {
        try (
                ByteArrayOutputStream bos = new ByteArrayOutputStream((int)file.length());
                BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))
        ) {
            int buf_size = end - start + 1 ;
            byte[] buffer = new byte[buf_size];
            int len = 0;
            in.skip(start);
            int notReadLength = buf_size;
            while(notReadLength > 0 && -1 != (len = in.read(buffer,0,buf_size))){
                bos.write(buffer,0,len);
                notReadLength = notReadLength - len;
                if (notReadLength <= buf_size) {
                    buf_size = notReadLength;
                }
            }
            return bos.toByteArray();
        }
    }

    public static byte[] readFromFile(File file, long start, long end) throws IOException {
        try (
                ByteArrayOutputStream bos = new ByteArrayOutputStream((int)file.length());
                BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))
        ) {
            int bufferSize = (int)(64 * KB);
            byte[] buffer = new byte[bufferSize];
            long notReadLength = end - start + 1;

            if (notReadLength <= bufferSize) {
                bufferSize = (int)notReadLength;
            }

            int readLength;
            in.skip(start);
            while(notReadLength > 0 && -1 != (readLength = in.read(buffer,0, bufferSize))){
                bos.write(buffer,0, readLength);
                notReadLength = notReadLength - readLength;
                if (notReadLength <= bufferSize) {
                    bufferSize = (int)notReadLength;
                }
            }
            return bos.toByteArray();
        }
    }

    public static byte[] readFromFile(File file) throws IOException {
        try (
                ByteArrayOutputStream bos = new ByteArrayOutputStream((int) file.length());
                BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))
        ){
            int buf_size = 1024;
            byte[] buffer = new byte[buf_size];
            int len;
            while(-1 != (len = in.read(buffer,0,buf_size))){
                bos.write(buffer,0,len);
            }
            return bos.toByteArray();
        }
    }

    public static File writeToFile(InputStream inputStream, File file) throws IOException {
        return writeToFile(inputStream, file, false);
    }

    public static File writeToFile(InputStream inputStream, File file, boolean append) throws IOException {
        logger.info("Write input stream to file {}.", file);

        try (FileOutputStream fos = new FileOutputStream(file, append)) {
            int read;
            byte[] bytes = new byte[1024];
            while ((read = inputStream.read(bytes)) != -1) {
                fos.write(bytes, 0, read);
            }
            return file;
        }
    }

    public static String readStringFromFile(String pathToFile) throws IOException {
        File file = new File(pathToFile);
        return readStringFromFile(file);
    }

    public static String readStringFromFile(File file) throws IOException {
        try (Reader reader = new InputStreamReader(new FileInputStream(file))) {
            String result = "";
            int tempchar;
            int numRead = 0;
            while ((tempchar = reader.read()) != -1) {
                result = result + (char) tempchar;
                if (result.length() > numRead + 10 * KB) {
                    logger.info("Result length: {}", result.length());
                    numRead = result.length();
                }
            }
            return result;

        }
    }

    public static String readStringFromFile(String pathToFile, long start, long end) throws IOException {
        return readStringFromFile(new File(pathToFile), start, end);
    }

    public static String readStringFromFile(File file, long start, long end) throws IOException {
        try (Reader reader = new InputStreamReader(new FileInputStream(file))) {
            String result = "";
            reader.skip(start);
            int tempChar;
            while (((tempChar = reader.read()) != -1) && end >= start) {
                result = result + (char) tempChar;
                start++;
            }
            return result;
        }
    }

    public static boolean isTextFileType(String fileType) {
        return fileType.equals(TEXT_PLAIN)
                || fileType.equals(APPLICATION_JSON)
                || fileType.equals(APPLICATION_XML)
                || fileType.equals(TEXT_HTML)
                || fileType.equals(TEXT_XML);
    }

    public static String getFilePreview(File file) throws IOException {
        String contentPreview = "N/A for preview";
        String fileType;
        fileType = Files.probeContentType(file.toPath());

        if(fileType == null) {
            logger.warn("Failed to probe content type of file {}.", file);
            return "";
        }

        if(FileUtil.isTextFileType(fileType)) {
            BufferedReader br = new BufferedReader(new FileReader(file));
            contentPreview = br.readLine();
        }

        return contentPreview;
    }

    public static void copyFileRange(File sourceFile, long start, long end, File destFile) throws IOException {
        logger.info("Copy file {} range {} - {} to file {}", sourceFile, start, end, destFile);

        try (
                InputStream is = new FileInputStream(sourceFile);
                OutputStream os = new FileOutputStream(destFile)
        ) {
            is.skip(start);
            int readChar;
            while (((readChar = is.read()) != -1) && end >= start) {
                os.write(readChar);
                start++;
            }
        }
    }

    public static File copyFileRange(File sourceFile, Long start, Long end) throws IOException {
        String pathToRangeFile = sourceFile.getPath() + "-" + start;
        if(end != null) {
            pathToRangeFile += "-" + end;
        }
        File rangeFile = new File(pathToRangeFile);
        copyFileRange(sourceFile, start, end, rangeFile);
        return rangeFile;
    }

    public static boolean deleteDir(File dir) {
        logger.info("Clean up {}", dir.getPath());
        if (dir.isDirectory()) {
            String[] children = dir.list();

            for (String child : ThreadUtil.requireNonNull(children)) {
                boolean success = deleteDir(new File(dir, child));
                if (!success) {
                    return false;
                }
            }
        }

        return dir.delete();
    }

    /**
     * Create one temporary file
     * @return
     */
    public static File createTempFile() {
        File tmpdir = new File(System.getProperty("java.io.tmpdir"));

        File tempFile = new File(tmpdir + File.separator
                + UUID.randomUUID().toString());
        while (tempFile.exists()) {
            tempFile = new File(tmpdir + File.separator
                    + UUID.randomUUID().toString());
        }
        tempFile.deleteOnExit();
        return tempFile;
    }

    public static File getResourceFile(Class clazz, String pathToResourceFile) {
        return new File(clazz.getClassLoader().getResource(pathToResourceFile).getFile());
    }

    public static byte[] loadIntoMemory(File file) throws ExceedingBoundaryException, IOException {
        if(file.length() > Integer.MAX_VALUE) {
            throw new ExceedingBoundaryException("File is too large (> 2 GB).");
        }

        return IOUtils.toByteArray(new FileInputStream(file));
    }

    public static void main(String[] args) throws IOException {
        byte[] content = generateLine(1, "hahaha", 50, 50);
        logger.info("Content: {}", new String(content));
        logger.info("Length: {}", content.length);

        File file = new File(TMP_DIRECTORY_PATH + File.separator + "tmpfile");
        logger.info("File size: {}", generateFileContent(file, 10240).length());
    }

//    public static String convertStreamToString(InputStream is) {
//        /*
//          * To convert the InputStream to String we use the BufferedReader.readLine()
//          * method. We iterate until the BufferedReader return null which means
//          * there's no more data to read. Each line will appended to a StringBuilder
//          * and returned as String.
//          */
//        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
//        StringBuilder sb = new StringBuilder();
//        int ch;
//        try {
//            while ((ch = reader.read()) != -1) {
//                sb.append((char) ch);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                is.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        return sb.toString();
//    }

//    public static void main(String[] args) {
//        File file1 = new File("/home/zhangyue182/5G.gz");
//        long time1 = System.currentTimeMillis();
//        byte[] read1 = readFromFile_Retired(file1, (int)(1 * KB), (int)(300 * MB));
//        long time2 = System.currentTimeMillis();
//        byte[] read2 = readFromFile(file1, (int)(1 * KB), (int)(300 * MB));
//        long time3 = System.currentTimeMillis();
//
//        TestCase.assertEquals(read1, read2, "File content read differs");
//
//        logger.info("Read1: {}", time2 - time1);
//        logger.info("Read2: {}", time3 - time2);
//    }

}
