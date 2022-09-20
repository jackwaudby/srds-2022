package action;

import event.CommitOperationEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import state.NodeState;
import utils.Common;
import utils.Config;
import utils.EventList;
import utils.FailureRepairEventList;
import utils.Metrics;
import utils.Rand;

import java.util.Objects;

public class CommitOperationAction
{
    public static void commit( CommitOperationEvent event, Cluster cluster, Config config, EventList eventList, Rand rand, Metrics metrics,
                               FailureRepairEventList failureRepairEventList )
    {
        // epoch terminates due to failure and abort event completed before the commit event
        var currentEpoch = cluster.getCurrentEpoch();
        var originEpoch = event.getEpoch();
        if ( originEpoch < currentEpoch )
        {
            return;
        }

        var thisEventTime = event.getEventTime();

        switch ( cluster.getCurrentEpochState() )
        {
        // this is only possible if the epoch has been skipped
        case PROCESSING -> {
            if ( cluster.isSkipped() )
            {
                // generate the completed jobs in the completed epoch per non-failed node
                for ( int i = 0; i < config.getClusterSize(); i++ )
                {
                    if ( cluster.getNodeState( i ) == NodeState.OPERATIONAL )
                    {
                        cluster.incCompletedJobs( i, rand.getCompletedJobs() );
                    }
                }
                cluster.complete( metrics, thisEventTime, false, config );
                cluster.resetClusterState( config, metrics );
                Common.skipOrGenerateNextEpoch( thisEventTime, rand, eventList, cluster, config, failureRepairEventList );
            }
        }
        case COMMITTING -> {
            // if multi-commit enabled, at least 1 operational commit group, and there was a failure then it is a partial epoch
            cluster.complete( metrics, thisEventTime,
                    Objects.equals( config.getAlgorithm(), "multi" ) &&
                    cluster.getNumberOfOperationalCommitGroups() > 0 &&
                    !cluster.isNodeFailureSetEmpty(), config );
            cluster.resetClusterState( config, metrics );
            Common.skipOrGenerateNextEpoch( thisEventTime, rand, eventList, cluster, config, failureRepairEventList );
        }
        case ABORTING, WAITING -> {
        }
        }
    }
}

