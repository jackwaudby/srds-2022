package utils;

import org.apache.commons.math3.distribution.ExponentialDistribution;

import java.util.HashSet;
import java.util.Random;

import static java.lang.Integer.MAX_VALUE;

public class Rand
{
    private static final Rand instance = new Rand();

    private final ExponentialDistribution transactionServiceRateDistribution;
    private final Random remoteParticipantDistribution;
    private final Random misc;

    private final double epochSize;
    private final int clusterSize;

    public static Rand getInstance()
    {
        return instance;
    }

    private Rand()
    {
        var config = Config.getInstance();
        this.epochSize = config.getEpochTimeoutInSecs();
        this.clusterSize = config.getClusterSize();

        double transactionServiceRate = config.getTransactionServiceRateInSecs();
        this.transactionServiceRateDistribution = new ExponentialDistribution( transactionServiceRate );

        this.remoteParticipantDistribution = new Random();
        this.misc = new Random();

        long seedValue;
        if ( config.isSeedSet() )
        {
            seedValue = config.getSeedValue();
        }
        else
        {
            // choose random seed
            seedValue = misc.nextLong( 0, MAX_VALUE );
            config.setSeedValue( seedValue );
        }

        transactionServiceRateDistribution.reseedRandomGenerator( seedValue );
        remoteParticipantDistribution.setSeed( seedValue );
        misc.setSeed( seedValue );
    }

    public double generateNextEpoch()
    {
        return epochSize;
    }

    public double generateTransactionServiceTime()
    {
        return transactionServiceRateDistribution.sample();
    }

    public HashSet<Object> generateTpcCDependencies( int thisNodeId )
    {
        var n = misc.nextDouble();
        var remoteParticipantIds = new HashSet<>();

        if ( n < 0.5 )
        {
            // payment - pick 1 remote node
            var remoteParticipantId = remoteParticipantDistribution.nextInt( 0, this.clusterSize );
            while ( remoteParticipantId == thisNodeId )
            {
                remoteParticipantId = remoteParticipantDistribution.nextInt( 0, clusterSize );
            }
            remoteParticipantIds.add( remoteParticipantId );
        }
        else
        {
            // new order
            var items = misc.nextDouble( 5, 16 );

            for ( int i = 0; i < items; i++ )
            {
                var m = misc.nextDouble();

                if (m > .99)
                {

                    var remoteParticipantId = remoteParticipantDistribution.nextInt( 0, this.clusterSize );
                    while ( remoteParticipantId == thisNodeId )
                    {
                        remoteParticipantId = remoteParticipantDistribution.nextInt( 0, clusterSize );
                    }
                    remoteParticipantIds.add( remoteParticipantId );
                }
            }
        }

        return remoteParticipantIds;
    }
}
