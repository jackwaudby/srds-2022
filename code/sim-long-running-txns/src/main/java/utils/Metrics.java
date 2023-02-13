package utils;

import org.apache.log4j.Logger;
import state.Cluster;

import java.util.Objects;

public class Metrics {
    private final static Logger LOGGER = Logger.getLogger(Metrics.class.getName());

    private static final Metrics instance = new Metrics();

    private int completedEpochs;
    private int totallyFailedEpochs;
    private int partiallyFailedEpochs;

    private int cyclesWithFailures;
    private int cyclesWithMultipleFailures;

    private long completedJobs;
    private long lostCompletedJobs;
    private long lostInFlightJobs;
    private long lostAccessingCrashedNode;

    private double terminatedEpochsCumulativeLatency;

    private int failureEvents;
    private int cumulativeOperationalCommitGroups;

    private long committedTransactionsDuringFailures;

    private Metrics() {
        // epochs
        completedEpochs = 0;
        totallyFailedEpochs = 0;
        partiallyFailedEpochs = 0;

        // jobs
        completedJobs = 0;
        lostCompletedJobs = 0;
        lostInFlightJobs = 0;
        lostAccessingCrashedNode = 0;

        terminatedEpochsCumulativeLatency = 0.0;

        failureEvents = 0;

        cumulativeOperationalCommitGroups = 0;

        cyclesWithFailures = 0;
        cyclesWithMultipleFailures = 0;

        committedTransactionsDuringFailures = 0;
    }

    public static Metrics getInstance() {
        return instance;
    }

    public long getCommittedTransactionsDuringFailures() {
        return committedTransactionsDuringFailures;
    }

//    public int getCurrentEpoch(Cluster cluster) {
//        return cluster.getCurrentEpoch();
//    }

    public void incCompletedEpochs() {
        this.completedEpochs += 1;
    }

    public void incCompletedTransactions(int toAdd) {
        this.completedJobs += toAdd;
    }

    public int getFailureEvents() {
        return failureEvents;
    }

    public int getPartiallyFailedEpochs() {
        return partiallyFailedEpochs;
    }

    public int getTotallyFailedEpochs() {
        return totallyFailedEpochs;
    }

    public int getCompletedEpochs() {
        return completedEpochs;
    }

    public int getCumulativeOperationalCommitGroupsPerFailure() {
        return cumulativeOperationalCommitGroups;
    }

    public long getCompletedJobs() {
        return completedJobs;
    }

    public long getTotalLostJobs() {
        return lostCompletedJobs + lostInFlightJobs + lostAccessingCrashedNode;
    }

    public double getCompletedJobsPerSec() {
        return (double) getCompletedJobs() / terminatedEpochsCumulativeLatency;
    }

    public double getLostJobsPerSec() {
        return getTotalLostJobs() / terminatedEpochsCumulativeLatency;
    }

    public void incCumulativeLatency(double toAdd) {
        this.terminatedEpochsCumulativeLatency += toAdd;
    }

    public double getAverageLostJobsPerEpochWithFailure() {
        return (double) (lostInFlightJobs + lostCompletedJobs) / (double) getCyclesWithFailures();
    }

    public double getAverageNumberOfOperationalCommitGroupsPerFailure() {
        return (double) cumulativeOperationalCommitGroups / (double) getCyclesWithFailures();
    }

    public int getCyclesWithFailures() {
        return cyclesWithFailures;
    }

    public int getCyclesWithMultipleFailures() {
        return cyclesWithMultipleFailures;
    }

    public double getAverageCommittedTransactionsPerFailure() {
        return (double) getCommittedTransactionsDuringFailures() / (double) getCyclesWithFailures();
    }

    public void getSummary(Cluster cluster) {
        LOGGER.info("\nResults: ");
//        LOGGER.info("  current epoch: " + getCurrentEpoch(cluster));
        LOGGER.info("  completed epoch(s): " + getCompletedEpochs());
        LOGGER.info("  total jobs: " + (getCompletedJobs() + getTotalLostJobs()));
        LOGGER.info("  cumulative epoch latency: " + terminatedEpochsCumulativeLatency);
        LOGGER.info(String.format("  complete jobs/s: %.2f ", getCompletedJobsPerSec()));
    }
}
