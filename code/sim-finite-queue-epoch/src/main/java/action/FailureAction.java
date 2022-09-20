package action;

import event.FailureEvent;
import org.apache.log4j.Logger;
import state.ArrivalQueue;
import state.Cluster;
import state.EpochState;
import state.NodeState;
import utils.Common;
import utils.Config;
import utils.EventList;
import utils.Metrics;
import utils.Rand;

import java.util.Objects;

public class FailureAction
{
    private final static Logger LOGGER = Logger.getLogger( FailureAction.class.getName() );

    public static void fail( FailureEvent failureEvent, Config config, Cluster cluster, EventList eventList, Rand rand, Metrics metrics, ArrivalQueue queue )
    {

        var thisNodeId = failureEvent.getNodeId();
        var thisEventTime = failureEvent.getEventTime();
        var currentEpoch = cluster.getCurrentEpoch();
        System.out.println( "Failure in: " + currentEpoch );
        metrics.incFailureEvents();

        var currentEpochState = cluster.getCurrentEpochState();
        switch ( currentEpochState )
        {
        // failed during the processing of transactions
        case PROCESSING -> {
            // record there has been a failure in this epoch, used by epoch timeout
            cluster.recordFailureEvent( thisNodeId, thisEventTime, currentEpochState );

            // if the node was operational then retry job
            retryCurrentJobIfOperational( cluster, queue, thisNodeId, 0 );

            cluster.setNodeState( thisNodeId, NodeState.CRASHED );
        }
        // failed whilst waiting for jobs to finish
        case WAITING -> {
            // record this node as failed
            cluster.recordFailureEvent( thisNodeId, thisEventTime, currentEpochState );

            if ( Objects.equals( config.getAlgorithm(), "multi" ) )
            {
                retryCurrentJobIfOperational( cluster, queue, thisNodeId, 1 );
                cluster.setNodeState( thisNodeId, NodeState.CRASHED );

                // if all nodes now crashed then will not reach a ready-to-commit event
                if ( cluster.isClusterDown() )
                {
                    cluster.setCurrentEpochState( EpochState.ABORTING );
                    Common.generateAbortCompletionEvent( thisEventTime, rand, eventList, currentEpoch );
                }
            }
            // single: abort immediately
            else
            {
                // lost jobs for each transaction still with an in-flight job
                for ( int i = 0; i < config.getClusterSize(); i++ )
                {
                    retryCurrentJobIfOperational( cluster, queue, i, 2 );
                    cluster.setNodeState( i, NodeState.IDLE );
                }

                cluster.setNodeState( thisNodeId, NodeState.CRASHED );
                cluster.setCurrentEpochState( EpochState.ABORTING );
                Common.generateAbortCompletionEvent( thisEventTime, rand, eventList, currentEpoch );
            }
        }
        // fail before the commit operation completes
        case COMMITTING -> {
            // record this node as failed
            cluster.recordFailureEvent( thisNodeId, thisEventTime, currentEpochState );

            // set this node as crashed
            cluster.setNodeState( thisNodeId, NodeState.CRASHED );

            if ( Objects.equals( config.getAlgorithm(), "multi" ) )
            {
                // mark groups with crashed nodes as failed
                cluster.transitionNodesInCrashedGroupsToFailed();
                var operationalCommitGroups = cluster.getNumberOfOperationalCommitGroups();

                // every thing failed
                if ( operationalCommitGroups == 0 )
                {
                    cluster.setCurrentEpochState( EpochState.ABORTING );
                    Common.generateAbortCompletionEvent( thisEventTime, rand, eventList, currentEpoch );
                }
            }
            else
            {
                cluster.setCurrentEpochState( EpochState.ABORTING );
                Common.generateAbortCompletionEvent( thisEventTime, rand, eventList, currentEpoch );
            }
        }
        // there was another failure during the abort phase
        case ABORTING -> cluster.setNodeState( thisNodeId, NodeState.CRASHED );
        }
        Common.generateRepairEvent( thisNodeId, currentEpoch, thisEventTime, rand, eventList );
        Common.generateNextFailureEvent( thisEventTime, rand, eventList, cluster );
    }

    private static void retryCurrentJobIfOperational( Cluster cluster, ArrivalQueue queue, int thisNodeId, int i )
    {
        if ( cluster.getNodeState( thisNodeId ) == NodeState.OPERATIONAL )
        {
            cluster.incLostJobs( thisNodeId );
            var currentJob = cluster.getCurrentJob( thisNodeId );
            // ASSERT: must have job if not idle
            if ( currentJob == null )
            {
                System.out.println();
                System.out.println( "Current job unexpectedly null (FailureAction) " + i );
                System.out.println( thisNodeId );
                System.out.println( cluster.getNodeState( thisNodeId ) );
                System.out.println( cluster.getCurrentJob( thisNodeId ) );
                System.out.println( cluster.hasInFlightJob( thisNodeId ) );
                System.exit( 1 );
            }
            currentJob.incRetries();
            queue.addJob( currentJob );
        }
    }
}