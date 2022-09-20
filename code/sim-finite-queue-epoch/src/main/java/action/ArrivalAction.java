package action;

import event.ArrivalEvent;
import org.apache.log4j.Logger;
import state.ArrivalQueue;
import state.Cluster;
import state.EpochState;
import state.Job;
import state.NodeState;
import utils.Common;
import utils.EventList;
import utils.Metrics;
import utils.Rand;

public class ArrivalAction
{
    private final static Logger LOGGER = Logger.getLogger( ArrivalAction.class.getName() );

    public static void arrival( ArrivalEvent event, ArrivalQueue queue, Rand rand, EventList eventList, Cluster cluster )
    {
        var thisEventTime = event.getEventTime();
        var job = new Job( thisEventTime );

        Metrics.getInstance().incArrivals();

        var clusterState = cluster.getCurrentEpochState();
        // if cluster is in processing phase then
        if ( clusterState == EpochState.PROCESSING )
        {
            var idleNodeId = cluster.getIdleNodeId();
            // if there is no idle nodes add to queue
            if ( idleNodeId == -1 )
            {
                queue.addJob( job );
            }
            // else assign to idle node and generate a transaction completion event
            else
            {
                cluster.setNodeState( idleNodeId, NodeState.OPERATIONAL );
                cluster.setCurrentJob( idleNodeId, job );

                if ( cluster.getCurrentJob( idleNodeId ) == null )
                {
                    System.out.println();
                    System.out.println( "Job from queue should not be null" );
                    System.exit( 1 );
                }
                Common.generateTransactionCompletionEvent( rand, eventList, idleNodeId, thisEventTime, cluster.getCurrentEpoch() );
            }
        }
        // else cluster is waiting/aborting/committing, add to queue
        else
        {
            queue.addJob( job );
        }

        // generate next arrival
        Common.generateNextArrivalEvent( rand, eventList, thisEventTime );
    }
}
