package utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteOutResults {

    public static void writeOutResults(Config config, Metrics metrics, double realTime, double simulationTime) {

        String[] headers = {"cluster", "epochTimeout", "networkDelay", "shortTransactionServiceRate",
                "longTransactionServiceRate", "propLongTransactions", "propDistributedTransactions", "throughput",
                "realTime", "simTime"};

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
        var cluster = config.getClusterSize();
        var epochTimeout = config.getEpochTimeoutInMillis();
        var networkDelay = config.getNetworkDelayRateInMillis();
        var shortTransactionServiceRate = config.getShortTransactionServiceRateInMillis();
        var longTransactionServiceRate = config.getLongTransactionServiceRateInMillis();
        var propLongTransactions = config.getPropLongTransactions() * 100;
        var propDistributedTransactions = config.getPropDistributedTransactions() * 100;

        String params = String.format("%s,%s,%s,%s,%s,%s,%s", cluster, epochTimeout, networkDelay,
                shortTransactionServiceRate, longTransactionServiceRate, propLongTransactions, propDistributedTransactions);

        // main metrics
        var completedJobPs = metrics.getCompletedJobsPerSec();
        String main = String.format("%.4f", completedJobPs);

        BufferedWriter outputStream = null;
        FileWriter fileWriter;
        try {
            File file = new File("results.csv");
            String format = String.format("%s,%s,%.4f,%.4f", params, main, realTime, simulationTime);
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