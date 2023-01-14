package action;

import event.EventType;
import event.FailureEvent;
import event.RepairEvent;
import event.TransactionEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import state.NodeState;
import utils.EventList;
import utils.FailureRepairEventList;
import utils.Metrics;
import utils.Rand;

public class RepairAction
{
    private final static Logger LOGGER = Logger.getLogger( RepairAction.class.getName() );

    public static void repair( RepairEvent repairEvent, Cluster cluster, Metrics metrics, Rand rand, EventList eventList,
                               FailureRepairEventList failureRepairEventList )
    {
        failureRepairEventList.removeEvent( repairEvent );

        var thisNodeId = repairEvent.getNodeId();
        var thisEventTime = repairEvent.getEventTime();
        var currentEpoch = cluster.getCurrentEpoch();
        var originEpoch = repairEvent.getOriginEpoch();

        if ( currentEpoch == originEpoch )
        {
            // TODO: cannot fail and repair in the same epoch
            var repairEventTime = thisEventTime + rand.generateRepairTime();
            eventList.addEvent( new RepairEvent( repairEventTime, EventType.REPAIR, thisNodeId, originEpoch ) );
            failureRepairEventList.addEvent( new RepairEvent( repairEventTime, EventType.REPAIR, thisNodeId, originEpoch ) );

           // LOGGER.debug( String.format( " - generate repair event %.5f (ms)", repairEventTime * 1000.0 ) );
        }
        else
        {

            metrics.incRepairEvents();

            cluster.recordRepairEvent( thisNodeId );

            // case 1: if the whole cluster is down then restart and generate a failure event
            if ( cluster.isClusterDown() )
            {
        //        LOGGER.debug( " - cluster is down" );
                cluster.setNodeState( thisNodeId, NodeState.OPERATIONAL );

                generateTransactionCompletionEvent( rand, eventList, thisNodeId, thisEventTime, currentEpoch );

                generateFailureEvent( rand, eventList, thisNodeId, thisEventTime );
            }
            // case 2: cluster is operational
            else
            {
         //       LOGGER.debug( " - cluster is up" );

                var clusterState = cluster.getCurrentEpochState();

                switch ( clusterState )
                {
                // rejoin the cluster and process some transactions
                case PROCESSING -> {
                    cluster.setNodeState( thisNodeId, NodeState.OPERATIONAL );
                    generateTransactionCompletionEvent( rand, eventList, thisNodeId, thisEventTime, currentEpoch );
                }
                // epoch timed out, either waiting or committing either way the node just joins in
                case WAITING, COMMITTING -> {
                    cluster.setNodeState( thisNodeId, NodeState.READY );
                }
                // wait until next epoch
                case ABORTING -> cluster.setNodeState( thisNodeId, NodeState.OPERATIONAL );
                }
            }
        }
    }

    private static void generateTransactionCompletionEvent( Rand rand, EventList eventList, int thisNodeId, double thisEventTime, Integer currentEpoch )
    {
        var nextTransactionArrival = thisEventTime + rand.generateTransactionServiceTime();
        var nextTransactionEvent = new TransactionEvent( nextTransactionArrival, EventType.TRANSACTION_COMPLETED, thisNodeId, currentEpoch );
        eventList.addEvent( nextTransactionEvent );
       // LOGGER.debug( String.format( " - generate next transaction completion at %.5f (ms)", nextTransactionArrival * 1000.0 ) );
    }

    private static void generateFailureEvent( Rand rand, EventList eventList, int thisNodeId, double thisEventTime )
    {
        var nextFailureEventTime = thisEventTime + rand.generateNextFailure();
        var nextFailureEvent = new FailureEvent( nextFailureEventTime, EventType.FAILURE, thisNodeId );
        eventList.addEvent( nextFailureEvent );
    //    LOGGER.debug( String.format( " - generate next failure at %.5f (ms)", nextFailureEventTime * 1000.0 ) );
    }
}
