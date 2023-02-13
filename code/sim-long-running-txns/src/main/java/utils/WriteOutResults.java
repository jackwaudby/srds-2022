package utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteOutResults {

    public static void writeOutResults(Config config, Metrics metrics, double realTime, double simulationTime) {

        String[] headers = {"n", "a", "b", "muS", "muL", "k", "completedJobPs", "completedEp", "totalCompletedJobs", "realTime", "simTime"};

        StringBuilder headerStringBuilder = new StringBuilder();
        for (String header : headers) {
            headerStringBuilder.append(header).append(",");
        }
        String headerString = headerStringBuilder.toString();
        if (headerString.length() > 0) // remove trailing comma
        {
            headerString = headerString.substring(0, headerString.length() - 1);
        }

        // parameters
        var n = config.getClusterSize(); // cluster size
        var a = config.getEpochTimeoutInMillis(); // epoch timeout
        var b = config.getNetworkDelayRateInMillis(); // commit/abort delay
        var muShort = config.getShortTransactionServiceRateInMillis(); // transaction service rate
        var muLong = config.getLongTransactionServiceRateInMillis(); // transaction service rate
        var k = config.getPropLongTransactions() * 100; // proportion of distributed transactions
        String params = String.format("%s,%s,%s,%s,%s,%s", n, a, b, muShort, muLong, k);

        // main metrics
        var completedJobPs = metrics.getCompletedJobsPerSec();
        String main = String.format("%.4f", completedJobPs);

        // raw metrics
        var totalCompletedJobs = metrics.getCompletedJobs();
        String raw = String.format("%s", totalCompletedJobs);

        BufferedWriter outputStream = null;
        FileWriter fileWriter;
        try {
            File file = new File("results.csv");
            String format = String.format("%s,%s,%s,%.4f,%.4f", params, main, raw, realTime, simulationTime);
            if (!file.exists()) {
                fileWriter = new FileWriter(file, true);
                outputStream = new BufferedWriter(fileWriter);
                outputStream.append(headerString);
                outputStream.append("\n");
                outputStream.append(format);
                outputStream.append("\n");
            } else {
                fileWriter = new FileWriter(file, true);
                outputStream = new BufferedWriter(fileWriter);
                try {
                    outputStream.append(format);
                    outputStream.append("\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.flush();
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}