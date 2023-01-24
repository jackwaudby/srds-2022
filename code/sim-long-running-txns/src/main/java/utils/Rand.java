package utils;

import org.apache.commons.math3.distribution.ExponentialDistribution;

import java.util.Random;

public class Rand
{
    private static final Rand instance = new Rand();

    private final ExponentialDistribution shortTransactionServiceRateDistribution;
    private final ExponentialDistribution longTransactionServiceRateDistribution;

    private final ExponentialDistribution commitOperationDistribution;
    private final Random longTransactionDistribution;
    private final Random misc;

    private final double epochSize;
    private final double longTransactions;

    public static Rand getInstance()
    {
        return instance;
    }

    private Rand()
    {
        var config = Config.getInstance();
        this.epochSize = config.getEpochTimeoutInSecs();

        this.longTransactions = config.getPropLongTransactions();

        double longTransactionServiceRate = config.getLongTransactionServiceRateInSecs();
        this.longTransactionServiceRateDistribution = new ExponentialDistribution( longTransactionServiceRate );

        double shortTransactionServiceRate = config.getShortTransactionServiceRateInSecs();
        this.shortTransactionServiceRateDistribution = new ExponentialDistribution( shortTransactionServiceRate );

        double commitOperationRate = config.getCommitOperationRateInSecs();
        this.commitOperationDistribution = new ExponentialDistribution( commitOperationRate );

        this.longTransactionDistribution = new Random();
        this.misc = new Random();

        if ( config.isSeedSet() )
        {
            long seedValue = config.getSeedValue();

            shortTransactionServiceRateDistribution.reseedRandomGenerator( seedValue );
            longTransactionServiceRateDistribution.reseedRandomGenerator( seedValue );
            commitOperationDistribution.reseedRandomGenerator( seedValue );
            longTransactionDistribution.setSeed( seedValue );
            misc.setSeed( seedValue );
        }
    }

    public double generateNextEpochTimeout()
    {
        return epochSize;
    }

    public double generateShortTransactionServiceTime()
    {
        return shortTransactionServiceRateDistribution.sample();
    }

    public double generateLongTransactionServiceTime()
    {
        return longTransactionServiceRateDistribution.sample();
    }

    public double generateCommitOperationDuration()
    {
        return commitOperationDistribution.sample();
    }

    public boolean isLongTransaction()
    {
        var sample = longTransactionDistribution.nextDouble();

        return sample < longTransactions;
    }
}
