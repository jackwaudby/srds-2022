package action;

import event.EventType;
import event.FailureEvent;
import event.RepairEvent;
import org.apache.log4j.Logger;
import state.ArrivalQueue;
import state.Cluster;
import state.NodeState;
import utils.Common;
import utils.EventList;
import utils.Metrics;
import utils.Rand;

public class RepairAction
{
    private final static Logger LOGGER = Logger.getLogger( RepairAction.class.getName() );

    public static void repair( RepairEvent repairEvent, Cluster cluster, Metrics metrics, Rand rand, EventList eventList, ArrivalQueue queue )
    {

        var thisNodeId = repairEvent.getNodeId();
        var thisEventTime = repairEvent.getEventTime();
        var currentEpoch = cluster.getCurrentEpoch();
        var originEpoch = repairEvent.getOriginEpoch();

        if ( currentEpoch == originEpoch )
        {
            // Cannot fail and repair in the same epoch
            var repairEventTime = thisEventTime + rand.generateRepairTime();
            eventList.addEvent( new RepairEvent( repairEventTime, EventType.REPAIR, thisNodeId, originEpoch ) );
        }
        else
        {
            metrics.incRepairEvents();
            cluster.recordRepairEvent( thisNodeId );

            // if the whole cluster is down then restart and generate a failure event
            if ( cluster.isClusterDown() )
            {
                transitionToOperational( cluster, rand, eventList, queue, thisNodeId, thisEventTime, currentEpoch );
                generateFailureEvent( rand, eventList, thisNodeId, thisEventTime );
            }
            // cluster is operational
            else
            {
                var clusterState = cluster.getCurrentEpochState();
                switch ( clusterState )
                {
                // rejoin the cluster and process some transactions
                case PROCESSING -> transitionToOperational( cluster, rand, eventList, queue, thisNodeId, thisEventTime, currentEpoch );
                // epoch timed out, either waiting or committing either way the node just joins in
                case WAITING, COMMITTING -> cluster.setNodeState( thisNodeId, NodeState.READY );
                // wait until next epoch
                case ABORTING -> cluster.setNodeState( thisNodeId, NodeState.READY );
                }
            }
        }
    }

    private static void transitionToOperational( Cluster cluster, Rand rand, EventList eventList, ArrivalQueue queue, int thisNodeId, double thisEventTime,
                                                 Integer currentEpoch )
    {
        cluster.setNodeState( thisNodeId, NodeState.OPERATIONAL );

        Common.tryGetJobFromQueue( cluster, rand, eventList, queue, thisNodeId, thisEventTime, currentEpoch );
    }



    private static void generateFailureEvent( Rand rand, EventList eventList, int thisNodeId, double thisEventTime )
    {
        eventList.addEvent( new FailureEvent( thisEventTime + rand.generateNextFailure(), EventType.FAILURE, thisNodeId ) );
    }
}
