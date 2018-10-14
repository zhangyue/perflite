package ffzy.performance.report;

import ffzy.performance.util.DateUtil;
import ffzy.performance.util.FileUtil;
import ffzy.performance.util.MathUtil;

import java.io.File;

public class CsvReporter implements Reporter {
    File csvFile;

    public CsvReporter(File csvFile) {
        this.csvFile = csvFile;

        prepareFile();
        prepareHeader();
    }

    public void update(
            long date,
            long numRequests, float meanTps, float meanLatencyMillis,
            long cycleNumRequests, float cycleTps, float cycleLatencyMillis,
            int numFailedRequests, int cycleNumFailedRequests
    ) {
        String line = toCsv(
                date,
                numRequests, meanTps, meanLatencyMillis,
                cycleNumRequests, cycleTps, cycleLatencyMillis,
                numFailedRequests, cycleNumFailedRequests
        );

        System.out.print(line); // For Jenkins console.
        FileUtil.writeToFile(line, csvFile, true);
    }

    private void prepareFile() {
        csvFile.getParentFile().mkdirs();

        if(csvFile.exists()) {
            csvFile.renameTo(new File(csvFile.getPath() + "." + DateUtil.formatTime(System.currentTimeMillis(), DateUtil.FORMAT_SHORT_COMPACT)));
        }

        FileUtil.createNewFile(csvFile);
    }

    private void prepareHeader() {
        String line1 = "Sampling time,                ,Mean TPS,,Sampling request #,  ,Sampling latency, ,Sampling failed #,\n";
        String line2= "                   ,Request #,  ,Mean latency(ms),      ,Sampling TPS,          ,Failed #,,\n";

        System.out.print(line1); // For Jenkins console.
        System.out.print(line2);

        FileUtil.writeToFile(line1, csvFile, true);
        FileUtil.writeToFile(line2, csvFile, true);
    }

    private String toCsv(
            long date,
            long numRequests, float meanTps, float meanLatencyMillis,
            long cycleNumRequests, float cycleTps, float cycleLatencyMillis,
            int numFailedRequests, int cycleNumFailedRequests
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append(DateUtil.formatTime(date, DateUtil.FORMAT_SHORT_COMPACT)).append(", ");
        sb.append(String.format("%8d", numRequests)).append(", ");
        sb.append(String.format("%8.2f", MathUtil.round(meanTps, 2))).append(", ");
        sb.append(String.format("%8d", (int)meanLatencyMillis)).append(", ");
        sb.append(String.format("%8d", cycleNumRequests)).append(", ");
        sb.append(String.format("%8.2f", MathUtil.round(cycleTps, 2))).append(", ");
        sb.append(String.format("%8d", (int)cycleLatencyMillis)).append(", ");
        sb.append(String.format("%8d", numFailedRequests)).append(", ");
        sb.append(String.format("%8d", cycleNumFailedRequests)).append(", ");
        sb.append("\n");
        return sb.toString();
    }
}
