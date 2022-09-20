package action;

import event.CommitOperationEvent;
import org.apache.log4j.Logger;
import state.ArrivalQueue;
import state.Cluster;
import state.EpochState;
import utils.Common;
import utils.Config;
import utils.EventList;
import utils.Metrics;
import utils.Rand;

import java.util.Objects;

public class CommitOperationAction
{
    private final static Logger LOGGER = Logger.getLogger( CommitOperationAction.class.getName() );

    public static void commit( CommitOperationEvent event, Cluster cluster, Config config, EventList eventList, Rand rand, Metrics metrics, ArrivalQueue queue )
    {
        // if epoch has terminated due to failure and abort event then completed before the commit event
        var currentEpoch = cluster.getCurrentEpoch();
        var originEpoch = event.getEpoch();
        if ( originEpoch < currentEpoch )
        {
            return;
        }

        if ( cluster.getCurrentEpochState() == EpochState.COMMITTING )
        {

            var thisEventTime = event.getEventTime();
            var algo = config.getAlgorithm();

            cluster.complete( queue, config, metrics, thisEventTime,
                    Objects.equals( algo, "multi" ) && cluster.getNumberOfOperationalCommitGroups() > 0 && cluster.getNumberOfCrashedCommitGroups() != 0 );

            // reset current state
            cluster.resetClusterState( config, metrics );

            // skip or generate next epoch
            Common.generateNextEpochTimeoutEvent( thisEventTime, rand, eventList, cluster, config, queue, metrics );
        }
    }
}

