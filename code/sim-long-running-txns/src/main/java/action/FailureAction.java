package action;

import event.EventType;
import event.FailureEvent;
import event.RepairEvent;
import state.Cluster;
import state.NodeState;
import utils.Common;
import utils.Config;
import utils.EventList;
import utils.FailureRepairEventList;
import utils.Metrics;
import utils.Rand;

import java.util.Objects;

public class FailureAction
{
    public static void fail( FailureEvent failureEvent, Cluster cluster, EventList eventList, Rand rand, Metrics metrics,
                             FailureRepairEventList failureRepairEventList, Config config )
    {
        failureRepairEventList.removeEvent( failureEvent );

        var thisNodeId = failureEvent.getNodeId();
        var thisEventTime = failureEvent.getEventTime();
        var currentEpoch = cluster.getCurrentEpoch();

        metrics.incFailureEvents();
        cluster.recordFailureEvent( thisNodeId, thisEventTime );

        switch ( cluster.getCurrentEpochState() )
        {
        // case 1: failed during the processing of transactions
        // note, there can be multiple failures during this phase
        case PROCESSING -> {
            cluster.incInFlightJobsLost( thisNodeId );
            cluster.setNodeState( thisNodeId, NodeState.CRASHED );
        }
        // case 2: failed whilst waiting for jobs to finish
        case WAITING -> {
            // lost jobs for each transaction still with an in-flight job
//            for ( int i = 0; i < config.getClusterSize(); i++ )
//            {
//                if ( cluster.getNodeState( i ) == NodeState.OPERATIONAL )
//                {
//                    cluster.incInFlightJobsLost( i );
//                }
//            }
            cluster.incInFlightJobsLost( thisNodeId );
            cluster.setNodeState( thisNodeId, NodeState.CRASHED );
//            Common.transitionToAborting( cluster, eventList, rand, thisEventTime, currentEpoch );
        }
        // case 3: fail before the commit operation completes
        case COMMITTING -> {
            cluster.setNodeState( thisNodeId, NodeState.CRASHED );

            if ( Objects.equals( config.getAlgorithm(), "multi" ) )
            {
                // mark groups with crashed nodes as failed
                cluster.transitionNodesInCrashedGroupsToFailed();

                if ( cluster.getNumberOfOperationalCommitGroups() == 0 )
                {
                    Common.transitionToAborting( cluster, eventList, rand, thisEventTime, currentEpoch );
                }
            }
            else
            {
                Common.transitionToAborting( cluster, eventList, rand, thisEventTime, currentEpoch );
            }
        }
        // case 4: there was another failure during the abort phase
        // note, there can be multiple failures during this phase
        case ABORTING -> cluster.setNodeState( thisNodeId, NodeState.CRASHED );
        }

        generateRepairEvent( thisNodeId, currentEpoch, thisEventTime, rand, eventList, failureRepairEventList );
        generateNextFailureEvent( thisEventTime, rand, eventList, cluster, failureRepairEventList );
    }

    private static void generateRepairEvent( int thisNodeId, int currentEpoch, double thisEventTime, Rand rand, EventList eventList,
                                             FailureRepairEventList failureRepairEventList )
    {
        var repairEventTime = thisEventTime + rand.generateRepairTime();
        eventList.addEvent( new RepairEvent( repairEventTime, EventType.REPAIR, thisNodeId, currentEpoch ) );
        failureRepairEventList.addEvent( new RepairEvent( repairEventTime, EventType.REPAIR, thisNodeId, currentEpoch ) );
    }

    private static void generateNextFailureEvent( double thisEventTime, Rand rand, EventList eventList, Cluster cluster,
                                                  FailureRepairEventList failureRepairEventList )
    {

        // generate next failure event
        // edge case: only select from non-failed nodes
        if ( !cluster.isClusterDown() )
        {
            var nextFailedNode = rand.generateNodeId();
            var nextFailedNodeState = cluster.getNodeState( nextFailedNode );
            while ( nextFailedNodeState == NodeState.CRASHED )
            {
                nextFailedNode = rand.generateNodeId();
                nextFailedNodeState = cluster.getNodeState( nextFailedNode );
            }

            var nextFailureEventTime = thisEventTime + rand.generateNextFailure();
            eventList.addEvent( new FailureEvent( nextFailureEventTime, EventType.FAILURE, nextFailedNode ) );
            failureRepairEventList.addEvent( new FailureEvent( nextFailureEventTime, EventType.FAILURE, nextFailedNode ) );
        }
    }
}