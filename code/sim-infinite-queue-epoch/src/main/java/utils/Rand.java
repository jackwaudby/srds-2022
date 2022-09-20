package utils;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;

import java.util.Random;

public class Rand
{
    private static final Rand instance = new Rand();

    private final ExponentialDistribution transactionServiceRateDistribution;
    private final ExponentialDistribution failureDistribution;
    private final ExponentialDistribution repairDistribution;
    private final ExponentialDistribution commitOperationDistribution;
    private final ExponentialDistribution abortOperationDistribution;
    private final ExponentialDistribution epochTimeoutDistribution;

    private final PoissonDistribution nonFailedTransactionDistribution;

    private final Random remoteParticipantDistribution;
    private final Random distributedTransactionDistribution;
    private final Random nodeDistribution;
    private final Random misc;

    private final double epochSize;
    private final int clusterSize;
    private final double distTxn;
    private final boolean fixedEpochTimeout;

    public static Rand getInstance()
    {
        return instance;
    }

    private Rand()
    {
        var config = Config.getInstance();
        this.epochSize = config.getEpochTimeoutInSecs();
        this.clusterSize = config.getClusterSize();
        this.distTxn = config.getPropDistributedTransactions();
        this.fixedEpochTimeout = config.isFixedEpochTimeout();

        double transactionServiceRate = config.getTransactionServiceRateInSecs();
        this.transactionServiceRateDistribution = new ExponentialDistribution( transactionServiceRate );

        double repairRate = config.getRepairRateInSecs();
        this.repairDistribution = new ExponentialDistribution( repairRate );

        double failureRate = config.getFailureRateInSecs();
        this.failureDistribution = new ExponentialDistribution( failureRate );

        double commitOperationRate = config.getCommitOperationRateInSecs();
        this.commitOperationDistribution = new ExponentialDistribution( commitOperationRate );

        double abortOperationRate = config.getAbortOperationRateInSecs();
        this.abortOperationDistribution = new ExponentialDistribution( abortOperationRate );

        this.epochTimeoutDistribution = new ExponentialDistribution( epochSize );

        this.remoteParticipantDistribution = new Random();
        this.distributedTransactionDistribution = new Random();
        this.nodeDistribution = new Random();
        this.misc = new Random();

        var lambda = epochSize / transactionServiceRate;
        this.nonFailedTransactionDistribution = new PoissonDistribution( lambda );

        if ( config.isSeedSet() )
        {
            long seedValue = config.getSeedValue();

            transactionServiceRateDistribution.reseedRandomGenerator( seedValue );
            repairDistribution.reseedRandomGenerator( seedValue );
            failureDistribution.reseedRandomGenerator( seedValue );
            commitOperationDistribution.reseedRandomGenerator( seedValue );
            abortOperationDistribution.reseedRandomGenerator( seedValue );
            remoteParticipantDistribution.setSeed( seedValue );
            distributedTransactionDistribution.setSeed( seedValue );
            nodeDistribution.setSeed( seedValue );
            nonFailedTransactionDistribution.reseedRandomGenerator( seedValue );
            epochTimeoutDistribution.reseedRandomGenerator( seedValue );
            misc.setSeed( seedValue );
        }
    }

    public double generateNextEpochTimeout()
    {
        var nextEpochTimeout = epochSize;
        if ( !fixedEpochTimeout )
        {
            nextEpochTimeout = epochTimeoutDistribution.sample();
        }
        return nextEpochTimeout;
    }

    public double generateTransactionServiceTime()
    {
        return transactionServiceRateDistribution.sample();
    }

    public int getCompletedJobs()
    {
        return nonFailedTransactionDistribution.sample();
    }

    public double generateCommitOperationDuration()
    {
        return commitOperationDistribution.sample();
    }

//    public double generateAbortOperationDuration()
//    {
//        return abortOperationDistribution.sample();
//    }

    public double generateNextFailure()
    {
        return failureDistribution.sample();
    }

    public double generateRepairTime()
    {
        return repairDistribution.sample();
    }

    public int generateDependency( int thisNodeId, boolean affinity )
    {
        int remoteParticipantId;
        if ( affinity )
        {
            var n = misc.nextDouble();
            if ( n < 0.9 )
            {
                if ( thisNodeId % 2 == 0 )
                {
                    remoteParticipantId = thisNodeId + 1;
                }
                else
                {
                    remoteParticipantId = thisNodeId - 1;
                }
            }
            else
            {
                remoteParticipantId = remoteParticipantDistribution.nextInt( 0, this.clusterSize );
                while ( remoteParticipantId == thisNodeId )
                {
                    remoteParticipantId = remoteParticipantDistribution.nextInt( 0, clusterSize );
                }
            }
        }
        else
        {
            remoteParticipantId = remoteParticipantDistribution.nextInt( 0, this.clusterSize );
            while ( remoteParticipantId == thisNodeId )
            {
                remoteParticipantId = remoteParticipantDistribution.nextInt( 0, clusterSize );
            }
        }
        return remoteParticipantId;
    }

    public int generateNodeId()
    {
        return nodeDistribution.nextInt( 0, clusterSize );
    }

    public boolean isDistributedTransaction()
    {
        var sample = distributedTransactionDistribution.nextDouble();

        return sample < distTxn;
    }
}
