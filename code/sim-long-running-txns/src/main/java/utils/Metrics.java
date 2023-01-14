package utils;

import org.apache.log4j.Logger;
import state.Cluster;

import java.util.Objects;

public class Metrics
{
    private final static Logger LOGGER = Logger.getLogger( Metrics.class.getName() );

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
    private int repairEvents;

    private int cumulativeOperationalCommitGroups;

    private long committedTransactionsDuringFailures;

    private Metrics()
    {
        // epochs
        completedEpochs = 0;
        totallyFailedEpochs = 0;
        partiallyFailedEpochs = 0;

        // jobs
        completedJobs = 0;
        lostCompletedJobs = 0;
        lostInFlightJobs = 0;
        // TODO: excluded -- why?
        lostAccessingCrashedNode = 0;

        terminatedEpochsCumulativeLatency = 0.0;

        failureEvents = 0;
        repairEvents = 0;

        cumulativeOperationalCommitGroups = 0;

        cyclesWithFailures = 0;
        cyclesWithMultipleFailures = 0;

        committedTransactionsDuringFailures = 0;
    }

    public static Metrics getInstance()
    {
        return instance;
    }

    public void incCommittedTransactionsDuringFailures( int toAdd )
    {
        this.committedTransactionsDuringFailures += toAdd;
    }

    public long getCommittedTransactionsDuringFailures()
    {
        return committedTransactionsDuringFailures;
    }

    public void incCyclesWithFailures()
    {
        this.cyclesWithFailures += 1;
    }

    public void incCyclesWithMultipleFailures()
    {
        this.cyclesWithMultipleFailures += 1;
    }

    public int getCurrentEpoch( Cluster cluster )
    {
        return cluster.getCurrentEpoch();
    }

    public void incTotallyFailedEpochs()
    {
        this.totallyFailedEpochs += 1;
    }

    public void incPartiallyFailedEpochs()
    {
        this.partiallyFailedEpochs += 1;
    }

    public void incCompletedEpochs()
    {
        this.completedEpochs += 1;
    }

    public void incFailureEvents()
    {
        this.failureEvents += 1;
    }

    public void incRepairEvents()
    {
        this.repairEvents += 1;
    }

    public void incCompletedTransactions( int toAdd )
    {
        this.completedJobs += toAdd;
    }

    public void incLostCompletedJobs( int toAdd )
    {
        this.lostCompletedJobs += toAdd;
    }

    public void incLostInFlightJobs( int toAdd )
    {
        this.lostInFlightJobs += toAdd;
    }

    public void incLostAccessingCrashedNode( int toAdd )
    {
        this.lostAccessingCrashedNode += toAdd;
    }

    public void incOperationalCommitGroups( int toAdd )
    {
        this.cumulativeOperationalCommitGroups += toAdd;
    }

    public int getRepairEvents()
    {
        return repairEvents;
    }

    public int getFailureEvents()
    {
        return failureEvents;
    }

    public int getPartiallyFailedEpochs()
    {
        return partiallyFailedEpochs;
    }

    public int getTotallyFailedEpochs()
    {
        return totallyFailedEpochs;
    }

    public int getCompletedEpochs()
    {
        return completedEpochs;
    }

    public int getCumulativeOperationalCommitGroupsPerFailure()
    {
        return cumulativeOperationalCommitGroups;
    }

    public long getCompletedJobs()
    {
        return completedJobs;
    }

    public long getTotalLostJobs()
    {
        return lostCompletedJobs + lostInFlightJobs + lostAccessingCrashedNode;
    }

    public double getCompletedJobsPerSec()
    {
        return (double) getCompletedJobs() / terminatedEpochsCumulativeLatency;
    }

    public double getLostJobsPerSec()
    {
        return getTotalLostJobs() / terminatedEpochsCumulativeLatency;
    }

    public void incCumulativeLatency( double toAdd )
    {
        this.terminatedEpochsCumulativeLatency += toAdd;
    }

    public double getAverageLostJobsPerEpochWithFailure()
    {
        return (double) (lostInFlightJobs + lostCompletedJobs) / (double) getCyclesWithFailures();
    }

    public double getAverageNumberOfOperationalCommitGroupsPerFailure()

    {
        return (double) cumulativeOperationalCommitGroups / (double) getCyclesWithFailures();
    }

    public int getCyclesWithFailures()
    {
        return cyclesWithFailures;
    }

    public int getCyclesWithMultipleFailures()
    {
        return cyclesWithMultipleFailures;
    }

    public double getAverageCommittedTransactionsPerFailure()
    {
        return (double) getCommittedTransactionsDuringFailures() / (double) getCyclesWithFailures();
    }

    public void getSummary( Config config, Cluster cluster )
    {
        LOGGER.info( "\nResults: " );

        LOGGER.info( "  current epoch: " + getCurrentEpoch( cluster ) );
        LOGGER.info( "  totally failed epoch(s): " + getTotallyFailedEpochs() );
        LOGGER.info( "  partially failed epoch(s): " + getPartiallyFailedEpochs() );
        LOGGER.info( "  completed epoch(s): " + getCompletedEpochs() );

        LOGGER.info( "  cycles with failures: " + getCyclesWithFailures() );
        LOGGER.info( "  cycles with multiple failures: " + getCyclesWithMultipleFailures() );

        LOGGER.info( "  failure event(s): " + getFailureEvents() );
        LOGGER.info( "  repair event(s): " + getRepairEvents() );

        LOGGER.info( "  total jobs: " + (getCompletedJobs() + getTotalLostJobs()) );

        LOGGER.info( "      completed jobs: " + getCompletedJobs() );
        LOGGER.info( String.format( "       lost jobs: %s", getTotalLostJobs() ) );
        LOGGER.info( String.format( "           lost completed jobs: %s", lostCompletedJobs ) );
        LOGGER.info( String.format( "           lost accessing crashed node: %s", lostAccessingCrashedNode ) );
        LOGGER.info( String.format( "           lost in-flight jobs: %s", lostInFlightJobs ) );
        LOGGER.info( String.format( "  lost jobs in epochs with a failure (exc. accessing crashed node): %s", lostCompletedJobs + lostInFlightJobs ) );
        LOGGER.info( String.format( "  committed jobs in epochs with a failure: %s", committedTransactionsDuringFailures ) );

        LOGGER.info( "  cumulative epoch latency: " + terminatedEpochsCumulativeLatency );

        LOGGER.info( String.format( "  complete jobs/s: %.2f ", getCompletedJobsPerSec() ) );
        LOGGER.info( String.format( "  lost jobs/s: %.2f ", getLostJobsPerSec() ) );

        LOGGER.info( String.format( "  jobs lost/failure: %.2f", getAverageLostJobsPerEpochWithFailure() ) );
        LOGGER.info( String.format( "  jobs committed/failure: %.2f", getAverageCommittedTransactionsPerFailure() ) );

        if ( Objects.equals( config.getAlgorithm(), "multi" ) )
        {
            LOGGER.info( "  average operational commit groups/failure: " + getAverageNumberOfOperationalCommitGroupsPerFailure() );
        }
    }
}
