package utils;

import event.AbortOperationEvent;
import event.CommitOperationEvent;
import event.EpochTimeoutEvent;
import event.EventType;
import event.TransactionEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import state.EpochState;
import state.NodeState;

public class Common
{
    private final static Logger LOGGER = Logger.getLogger( Common.class.getName() );

    public static void skipOrGenerateNextEpoch( double thisEventTime, Rand rand, EventList eventList, Cluster cluster, Config config,
                                                FailureRepairEventList failureRepairEventList )
    {
        var nextEpochTimeoutEvent = thisEventTime + rand.generateNextEpochTimeout();
        var nextCommitOperationEvent = nextEpochTimeoutEvent + rand.generateCommitOperationDuration();
        var currentEpoch = cluster.getCurrentEpoch();

        // if there is a failure or repair event in the next (epoch + commit operation) interval then
        var isFailureOrRepair = failureRepairEventList.isFailureOrRepairBefore( nextCommitOperationEvent );
        if ( isFailureOrRepair )
        {
            LOGGER.debug( " - simulate, there is a failure in the next epoch" );

            // generate epoch timeout event
            var epochTimeoutEvent = new EpochTimeoutEvent( nextEpochTimeoutEvent, EventType.EPOCH_TIMEOUT, currentEpoch );
            eventList.addEvent( epochTimeoutEvent );
            LOGGER.debug( String.format( " - generate next epoch timeout at %.5f (ms)", nextEpochTimeoutEvent * 1000.0 ) );

            // generate transactions for operational nodes
            var clusterSize = config.getClusterSize();
            for ( int i = 0; i < clusterSize; i++ )
            {
                if ( cluster.getNodeState( i ) == NodeState.OPERATIONAL )
                {
                    var nextTransactionCompletionTime = thisEventTime + rand.generateTransactionServiceTime();
                    var transactionEvent = new TransactionEvent( nextTransactionCompletionTime, EventType.TRANSACTION_COMPLETED, i, currentEpoch );
                    eventList.addEvent( transactionEvent );
                }
            }
            LOGGER.debug( " - generate transactions for operational nodes" );
        }
        // else generation of transactions in the next epoch can be skipped
        else
        {
            LOGGER.debug( " - skip, no failures in next epoch" );
            cluster.setSkipped( true );
            // jump to next commit operation completion
            var commitOperationEvent = new CommitOperationEvent( nextCommitOperationEvent, EventType.COMMIT_COMPLETED, currentEpoch );
            eventList.addEvent( commitOperationEvent );
        }
    }

    public static void transitionToAborting( Cluster cluster, EventList eventList, Rand rand, double thisEventTime, Integer currentEpoch )
    {
        cluster.setCurrentEpochState( EpochState.ABORTING );
        generateAbortCompletionEvent( thisEventTime, rand, eventList, currentEpoch );
    }

    private static void generateAbortCompletionEvent( double thisEventTime, Rand rand, EventList eventList, int currentEpoch )
    {
        var abortEventTime = thisEventTime + rand.generateCommitOperationDuration();
        eventList.addEvent( new AbortOperationEvent( abortEventTime, EventType.ABORT_COMPLETED, currentEpoch ) );
    }
}
