package utils;

import org.apache.log4j.Logger;
import state.Cluster;
import state.EpochSummary;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Metrics
{
    private final static Logger LOGGER = Logger.getLogger( Metrics.class.getName() );

    private static final Metrics instance = new Metrics();

    private int totallyFailedEpochs;
    private int completedEpochs;
    private int partialFailedEpochs;
    private long completedJobs;
    private double cumulativeEpochLatency;
    private int failureEvents;
    private int repairEvents;
    private int cumulativeOperationalCommitGroups;
    private int lostJobs;
    private int failedEpochLostJobs;
    private List<Integer> queueSizeAtEpochTimeout;
    private List<Double> perEpochAverageRespTime;
    private double cumulativeJobResponseTime;

    private int arrivals;
    private List<Integer> perEpochArrivals;

    private Metrics()
    {
        failureEvents = 0;
        cumulativeEpochLatency = 0.0;
        totallyFailedEpochs = 0;
        repairEvents = 0;
        partialFailedEpochs = 0;
        completedEpochs = 0;
        completedJobs = 0;
        cumulativeOperationalCommitGroups = 0;
        lostJobs = 0;
        failedEpochLostJobs = 0;
        queueSizeAtEpochTimeout = new ArrayList<>();
        perEpochAverageRespTime = new ArrayList<>();
        cumulativeJobResponseTime = 0.0;

        arrivals = 0;
        perEpochArrivals = new ArrayList<>();
    }

    public static Metrics getInstance()
    {
        return instance;
    }

    public void incArrivals()
    {
        this.arrivals += 1;
    }

    public void resetArrivals()
    {
        this.arrivals = 0;
    }

    public void recordPerEpochArrivals()
    {
        perEpochArrivals.add( arrivals );
        resetArrivals();
    }

    public List<Integer> getPerEpochArrivals()
    {
        return perEpochArrivals;
    }

    public int getCurrentEpoch()
    {
        return Cluster.getInstance().getCurrentEpoch();
    }

    public void incTotallyFailedEpochs()
    {
        this.totallyFailedEpochs += 1;
    }

    public void incPartialFailures()
    {
        this.partialFailedEpochs += 1;
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

    public void incCumulativeResponseTime( double toAdd )
    {
        this.cumulativeJobResponseTime += toAdd;
    }

    public int getFailureEvents()
    {
        return failureEvents;
    }

    public int getPartialFailedEpochs()
    {
        return partialFailedEpochs;
    }

    public int getTotallyFailedEpochs()
    {
        return totallyFailedEpochs;
    }

    public int getCompletedEpochs()
    {
        return completedEpochs;
    }

    public void incCompletedTransactions( int toAdd )
    {
        this.completedJobs += toAdd;
    }

    public void incLostJobs( int toAdd )
    {
        this.lostJobs += toAdd;
    }

    public void incFailedEpochLostJobs( int toAdd )
    {
        this.failedEpochLostJobs += toAdd;
    }

    public void incOperationalCommitGroups( int toAdd )
    {
        this.cumulativeOperationalCommitGroups += toAdd;
    }

    public double getAverageNumberOfOperationalCommitGroupsPerFailure()
    {
        return (double) cumulativeOperationalCommitGroups / (double) getFailureEvents();
    }

    public int getCumulativeOperationalCommitGroupsPerFailure()
    {
        return cumulativeOperationalCommitGroups;
    }

    public long getCompletedJobs()
    {
        return completedJobs;
    }

    public int getLostJobs()
    {
        return lostJobs;
    }

    public double getCompletedJobsPerSec()
    {
        return (double) getCompletedJobs() / cumulativeEpochLatency;
    }

    public double getLostJobsPerSec()
    {
        return lostJobs / cumulativeEpochLatency;
    }

    public void incCumulativeLatency( double toAdd )
    {
        this.cumulativeEpochLatency += toAdd;
    }

    public double getLostJobsPerFailure()
    {
        return (double) failedEpochLostJobs / (double) getFailureEvents();
    }

    public void addQueueSize( int size )
    {
        this.queueSizeAtEpochTimeout.add( size );
    }

    public List<Integer> getQueueSizeAtEpochTimeout()
    {
        return queueSizeAtEpochTimeout;
    }

    public void addPerEpochAvRespTime( double avRespTime )
    {
        this.perEpochAverageRespTime.add( avRespTime );
    }

    public List<Double> getPerEpochAvRespTime()
    {
        return perEpochAverageRespTime;
    }

    public double getAverageResponseTime()
    {
        return (cumulativeJobResponseTime / (double) completedJobs);
    }

    public void getSummary()
    {
        LOGGER.info( "Results: " );
        // Raw numbers
        LOGGER.info( "  current epoch: " + getCurrentEpoch() );
        LOGGER.info( "  failed epoch(s): " + totallyFailedEpochs );
        LOGGER.info( "  partial epoch(s): " + partialFailedEpochs );
        LOGGER.info( "  completed epoch(s): " + completedEpochs );
        LOGGER.info( "  failure event(s): " + failureEvents );
        LOGGER.info( "  repair event(s): " + repairEvents );
        LOGGER.info( "  completed jobs: " + completedJobs );
        LOGGER.info( "  lost jobs: " + lostJobs );
        LOGGER.info( "  lost jobs in epochs with a failure: " + failedEpochLostJobs );
        // LOGGER.info( "  cumulative epoch latency: " + cumulativeEpochLatency );
        // LOGGER.info( "  cumulative job response time: " + cumulativeJobResponseTime );

        // Metrics
        LOGGER.info( String.format( "  complete jobs/ms: %.5f ", getCompletedJobsPerSec() / 1000.0 ) );
        LOGGER.info( String.format( "  lost jobs/ms: %.5f ", getLostJobsPerSec() / 1000.0 ) );
        LOGGER.info( String.format( "  lost jobs/failure: %.5f", (double) failedEpochLostJobs / (double) failureEvents ) );
        LOGGER.info( String.format( "  average response time (ms): %.5f", getAverageResponseTime() * 1000 ) );

        var algo = Config.getInstance().getAlgorithm();
        if ( Objects.equals( algo, "multi" ) )
        {
            LOGGER.info( "  average operational commit groups/failure: " + getAverageNumberOfOperationalCommitGroupsPerFailure() );
        }
    }
}
