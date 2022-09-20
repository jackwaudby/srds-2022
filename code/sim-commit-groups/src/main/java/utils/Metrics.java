package utils;

import org.apache.log4j.Logger;

public class Metrics
{
    private final static Logger LOGGER = Logger.getLogger( Metrics.class.getName() );

    private static final Metrics instance = new Metrics();

    private int completedEpochs;
    private int cumulativeDistinctCommitGroups;

    private Metrics()
    {
        completedEpochs = 0;
        cumulativeDistinctCommitGroups = 0;
    }

    public static Metrics getInstance()
    {
        return instance;
    }

    public void incCompletedEpochs()
    {
        this.completedEpochs += 1;
    }

    public int getCompletedEpochs()
    {
        return completedEpochs;
    }

    public void incNumberOfCommitGroups( int toAdd )
    {
        this.cumulativeDistinctCommitGroups += toAdd;
    }

    public double getAverageNumberOfCommitGroupsPerEpoch()
    {
        return (double) cumulativeDistinctCommitGroups / (double) completedEpochs;
    }

    public void getSummary()
    {
        LOGGER.info( "Results: " );
        LOGGER.info( "  completed epoch(s): " + getCompletedEpochs() );
        LOGGER.info( "  average number of commit groups/epoch: " + getAverageNumberOfCommitGroupsPerEpoch() );
    }
}
