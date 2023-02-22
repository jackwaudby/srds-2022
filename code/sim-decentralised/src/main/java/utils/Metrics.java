package utils;

import org.apache.log4j.Logger;
import state.Cluster;

public class Metrics {
    private final static Logger LOGGER = Logger.getLogger(Metrics.class.getName());

    private static final Metrics instance = new Metrics();

    private Metrics() {
    }

    public static Metrics getInstance() {
        return instance;
    }

    public long getCompletedJobs() {
        var bucket = 0;
        var nodes = Cluster.getInstance().getNodes();
        for (var node : nodes) {
            var epochs = node.getEpochs();
            for (var epoch : epochs) {
                bucket += epoch.getCompletedTransactions();
            }
        }
        return bucket;
    }

    public double getCumulativeLatency() {
        return Cluster.getInstance().getStop();
    }

    public double getCompletedJobsPerSec() {
        return (double) getCompletedJobs() / getCumulativeLatency();
    }

    public void getSummary() {
        LOGGER.info("\nResults: ");
        LOGGER.info("  completed transactions: " +  getCompletedJobs());
        LOGGER.info("  cumulative latency: " + getCumulativeLatency());
        LOGGER.info(String.format("  completed txn/s: %.2f ", getCompletedJobsPerSec()));
    }
}
