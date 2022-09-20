package utils;

import event.AbortOperationEvent;
import event.ArrivalEvent;
import event.CommitOperationEvent;
import event.EpochTimeoutEvent;
import event.EventType;
import event.FailureEvent;
import event.RepairEvent;
import event.TransactionEvent;
import org.apache.log4j.Logger;
import state.ArrivalQueue;
import state.Cluster;
import state.NodeState;

public class Common
{
    private final static Logger LOGGER = Logger.getLogger( Common.class.getName() );

    public static void generateNextEpochTimeoutEvent( double thisEventTime, Rand rand, EventList eventList, Cluster cluster, Config config, ArrivalQueue queue,
                                                      Metrics metrics )
    {

        // record the queue size at the end of the epoch cycle
        metrics.addQueueSize( queue.getQueueSize() );

        // generate epoch timeout event
        var currentEpoch = cluster.getCurrentEpoch();
        eventList.addEvent( new EpochTimeoutEvent( thisEventTime + rand.generateNextEpochTimeout(), EventType.EPOCH_TIMEOUT, currentEpoch ) );

        // if there is a job in the queue then generate a transaction completion if the node is idle
        for ( int nodeId = 0; nodeId < config.getClusterSize(); nodeId++ )
        {
            if ( !queue.isQueueEmpty() && cluster.getNodeState( nodeId ) == NodeState.IDLE )
            {
                cluster.setNodeState( nodeId, NodeState.OPERATIONAL );

                var job = queue.getJob();
                //ASSERT: must be a job to create a completion event
                if ( job == null )
                {
                    System.out.println();
                    System.out.println( "Job should not be null" );
                    System.exit( 1 );
                }
                cluster.setCurrentJob( nodeId, job );
                generateTransactionCompletionEvent( rand, eventList, nodeId, thisEventTime, currentEpoch );
            }
        }
    }

    public static void generateCommitOperationCompletionEvent( EventList eventList, Rand rand, double thisEventTime, int currentEpoch )
    {
        var commitOperationEventTime = thisEventTime + rand.generateCommitOperationDuration();
        var commitOperationEvent = new CommitOperationEvent( commitOperationEventTime, EventType.COMMIT_COMPLETED, currentEpoch );
        eventList.addEvent( commitOperationEvent );
    }

    public static void generateAbortCompletionEvent( double thisEventTime, Rand rand, EventList eventList, int currentEpoch )
    {
        eventList.addEvent( new AbortOperationEvent( thisEventTime + rand.generateAbortOperationDuration(), EventType.ABORT_COMPLETED, currentEpoch ) );
    }

    public static void generateTransactionCompletionEvent( Rand rand, EventList eventList, int thisNodeId, double thisEventTime, Integer currentEpoch )
    {
        eventList.addEvent(
                new TransactionEvent( thisEventTime + rand.generateTransactionServiceTime(), EventType.TRANSACTION_COMPLETED, thisNodeId, currentEpoch ) );
    }

    public static void generateRepairEvent( int thisNodeId, int currentEpoch, double thisEventTime, Rand rand, EventList eventList )
    {
        eventList.addEvent( new RepairEvent( thisEventTime + rand.generateRepairTime(), EventType.REPAIR, thisNodeId, currentEpoch ) );
    }

    public static void generateNextFailureEvent( double thisEventTime, Rand rand, EventList eventList, Cluster cluster )
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
        }
    }

    public static void generateNextArrivalEvent( Rand rand, EventList eventList, double thisEventTime )
    {
        eventList.addEvent( new ArrivalEvent( thisEventTime + rand.generateNextArrivalTime(), EventType.TRANSACTION_ARRIVAL ) );
    }

    public static void tryGetJobFromQueue( Cluster cluster, Rand rand, EventList eventList, ArrivalQueue queue, int thisNodeId, double thisEventTime,
                                           Integer currentEpoch )
    {
        if ( !queue.isQueueEmpty() )
        {
            var job = queue.getJob();
            // ASSERT: must receive a job to create a transaction
            if ( job == null )
            {
                System.out.println();
                System.out.println( "Should be a job in the queue (Common)" );
                System.exit( 1 );
            }
            cluster.setCurrentJob( thisNodeId, job );
            Common.generateTransactionCompletionEvent( rand, eventList, thisNodeId, thisEventTime, currentEpoch );
        }
        else
        {
            cluster.setNodeState( thisNodeId, NodeState.IDLE );
            cluster.setCurrentJob( thisNodeId, null );
        }
    }
}
