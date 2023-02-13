package utils;

import org.apache.commons.math3.distribution.ExponentialDistribution;

import java.util.Random;

public class Rand {
    private static final Rand instance = new Rand();

    private final ExponentialDistribution shortTransactionServiceRateDistribution;
    private final ExponentialDistribution longTransactionServiceRateDistribution;
    private final ExponentialDistribution networkDelayDistribution;
    private final Random longTransactionDistribution;
    private final Random distributedTransactionDistribution;
    private final Random remoteParticipantDistribution;
    private final Random misc;

    private final double epochSize;
    private final int clusterSize;
    private final double proportionOfDistributedTransactions;
    private final double longTransactions;

    public static Rand getInstance() {
        return instance;
    }

    private Rand() {
        var config = Config.getInstance();
        this.clusterSize = config.getClusterSize();
        this.epochSize = config.getEpochTimeoutInSecs();
        this.proportionOfDistributedTransactions = config.getPropDistributedTransactions();
        this.longTransactions = config.getPropLongTransactions();

        double longTransactionServiceRate = config.getLongTransactionServiceRateInSecs();
        this.longTransactionServiceRateDistribution = new ExponentialDistribution(longTransactionServiceRate);

        double shortTransactionServiceRate = config.getShortTransactionServiceRateInSecs();
        this.shortTransactionServiceRateDistribution = new ExponentialDistribution(shortTransactionServiceRate);

        double networkDelayRate = config.getNetworkDelayRateInSecs();
        this.networkDelayDistribution = new ExponentialDistribution(networkDelayRate);

        this.longTransactionDistribution = new Random();
        this.remoteParticipantDistribution = new Random();
        this.distributedTransactionDistribution = new Random();
        this.misc = new Random();


        if (config.isSeedSet()) {
            long seedValue = config.getSeedValue();

            shortTransactionServiceRateDistribution.reseedRandomGenerator(seedValue);
            longTransactionServiceRateDistribution.reseedRandomGenerator(seedValue);
            networkDelayDistribution.reseedRandomGenerator(seedValue);
            longTransactionDistribution.setSeed(seedValue);
            remoteParticipantDistribution.setSeed(seedValue);
            distributedTransactionDistribution.setSeed(seedValue);
            misc.setSeed(seedValue);
        }
    }

    public double generateNextEpochTimeout() {
        return epochSize;
    }

    public double generateShortTransactionServiceTime() {
        return shortTransactionServiceRateDistribution.sample();
    }

    public double generateLongTransactionServiceTime() {
        return longTransactionServiceRateDistribution.sample();
    }

    public double generateNetworkDelayDuration() {
        return networkDelayDistribution.sample();
    }

    public boolean isLongTransaction() {
        var sample = longTransactionDistribution.nextDouble();

        return sample < longTransactions;
    }

    public int generateDependency(int thisNodeId) {
        int remoteParticipantId = remoteParticipantDistribution.nextInt(0, this.clusterSize);
        while (remoteParticipantId == thisNodeId) {
            remoteParticipantId = remoteParticipantDistribution.nextInt(0, clusterSize);
        }
        return remoteParticipantId;
    }

    public boolean isDistributedTransaction() {
        return distributedTransactionDistribution.nextDouble() < proportionOfDistributedTransactions;
    }
}
