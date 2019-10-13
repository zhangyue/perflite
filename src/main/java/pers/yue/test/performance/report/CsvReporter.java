package pers.yue.test.performance.report;

import pers.yue.test.util.FileTestUtil;
import pers.yue.util.DateUtil;
import pers.yue.util.MathUtil;

import java.io.File;

/**
 * Dump the test statistics into CSV file.
 *
 * Created by zhangyue182 on 2018/08/28
 */
public class CsvReporter implements Reporter {
    private File csvFile;

    private boolean isHeaderPrinted = false;

    private static final int numLinesToRepeatHeader = 50;
    private int linesAfterLastHeader = 0;

    private static final String line1 = "Sampling time,                ,Mean TPS,,Sampling request #,  ,Sampling latency,              ,tp95,          ,Failed #,         ,\n";
    private static final String line2 = "                   ,Request #,  ,Mean latency(ms),      ,Sampling TPS,              ,tp90,              ,tp99, ,Sampling failed #,\n";

    public CsvReporter(File csvFile) {
        this.csvFile = csvFile;

        prepareFile();
    }

    public void update(
            long date,
            long numRequests, float meanTps, float meanLatencyMillis,
            long cycleNumRequests, float cycleTps, float cycleLatencyMillis,
            int numFailedRequests, int cycleNumFailedRequests,
            long tp90, long tp95, long tp99
    ) {
        if(!isHeaderPrinted) {
            printHeader();
            isHeaderPrinted = true;
        }

        if(linesAfterLastHeader++ == numLinesToRepeatHeader) {
            repeatHeader();
            linesAfterLastHeader = 0;
        }

        String line = toCsv(
                date,
                numRequests, meanTps, meanLatencyMillis,
                cycleNumRequests, cycleTps, cycleLatencyMillis,
                numFailedRequests, cycleNumFailedRequests,
                tp90, tp95, tp99
        );

        System.out.print(line); // For Jenkins console.
        FileTestUtil.writeToFile(line, csvFile, true);
    }

    private void prepareFile() {
        csvFile.getParentFile().mkdirs();

        if(csvFile.exists()) {
            csvFile.renameTo(new File(csvFile.getPath() + "." + DateUtil.formatTime(System.currentTimeMillis(), DateUtil.FORMAT_SHORT_COMPACT)));
        }

        FileTestUtil.createNewFile(csvFile);
    }

    @Override
    public void printHeader() {
        System.out.print(line1); // For Jenkins console.
        System.out.print(line2);

        FileTestUtil.writeToFile(line1, csvFile, true);
        FileTestUtil.writeToFile(line2, csvFile, true);
    }

    @Override
    public void repeatHeader() {
        System.out.print(line1); // For Jenkins console.
        System.out.print(line2);
    }

    private String toCsv(
            long date,
            long numRequests, float meanTps, float meanLatencyMillis,
            long cycleNumRequests, float cycleTps, float cycleLatencyMillis,
            int numFailedRequests, int cycleNumFailedRequests,
            long tp90, long tp95, long tp99
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append(DateUtil.formatTime(date, DateUtil.FORMAT_SHORT_COMPACT)).append(", ");
        sb.append(String.format("%8d", numRequests)).append(", ");
        sb.append(String.format("%8.2f", MathUtil.round(meanTps, 2))).append(", ");
        sb.append(String.format("%8d", (int)meanLatencyMillis)).append(", ");
        sb.append(String.format("%8d", cycleNumRequests)).append(", ");
        sb.append(String.format("%8.2f", MathUtil.round(cycleTps, 2))).append(", ");
        sb.append(String.format("%8d", (int)cycleLatencyMillis)).append(", ");
        sb.append(String.format("%8d", tp90)).append(", ");
        sb.append(String.format("%8d", tp95)).append(", ");
        sb.append(String.format("%8d", tp99)).append(", ");
        sb.append(String.format("%8d", numFailedRequests)).append(", ");
        sb.append(String.format("%8d", cycleNumFailedRequests)).append(", ");
        sb.append("\n");
        return sb.toString();
    }
}
