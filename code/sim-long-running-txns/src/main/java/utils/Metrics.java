package utils;

import org.apache.log4j.Logger;
import state.Cluster;

public class Metrics {
    private final static Logger LOGGER = Logger.getLogger(Metrics.class.getName());

    private static final Metrics instance = new Metrics();

    private long completedJobs;


    private Metrics() {
    }

    public static Metrics getInstance() {
        return instance;
    }

    public long getCompletedJobs() {
        Cluster.getInstance().getNodes().forEach(node ->
                node.getEpochs().forEach(epoch -> completedJobs += epoch.getCompletedTransactions()));

        return completedJobs;
    }

    public double getCumulativeLatency() {
        return Cluster.getInstance().getStop();
    }

    public double getCompletedJobsPerSec() {
        return (double) getCompletedJobs() / getCumulativeLatency();
    }

    public void getSummary() {
        LOGGER.info("\nResults: ");
        LOGGER.info("  completed txns: " + completedJobs);
        LOGGER.info("  cumulative latency: " + getCumulativeLatency());
        LOGGER.info(String.format("  completed txn/s: %.2f ", getCompletedJobsPerSec()));
//        Cluster.getInstance().getNodes().forEach(node -> node.getEpochs().forEach(epoch -> System.out.printf("Node: %s, epoch: %s\n", node.getId(), epoch)));
    }
}
