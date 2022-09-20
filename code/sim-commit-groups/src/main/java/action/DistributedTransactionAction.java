package action;

import event.EventType;
import event.ReadyToCommitEvent;
import event.DistributedTransactionEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import state.NodeState;
import utils.Config;
import utils.EventList;
import utils.Rand;

public class DistributedTransactionAction
{
    private final static Logger LOGGER = Logger.getLogger( DistributedTransactionAction.class.getName() );

    public static void execute( DistributedTransactionEvent event, Cluster cluster, Config config, EventList eventList, Rand rand )
    {
        var thisNodeId = event.getNodeId();
        var currentEpochState = cluster.getCurrentEpochState();
        var thisEventTime = event.getEventTime();

        switch ( currentEpochState )
        {
        case PROCESSING -> {
            var dependencyNodeIds = rand.generateTpcCDependencies( thisNodeId );
            dependencyNodeIds.forEach( dependencyNodeId -> cluster.addDependency( thisNodeId, (Integer) dependencyNodeId ) );
            generateNextDistributedTransactionCompletionEvent( eventList, rand, thisNodeId, thisEventTime );
        }
        case WAITING -> {
            cluster.setNodeState( thisNodeId, NodeState.READY );

            if ( cluster.areAllNodesReadyToCommit( config ) )
            {
                var readyToCommitEvent = new ReadyToCommitEvent( thisEventTime, EventType.READY_TO_COMMIT );
                eventList.addEvent( readyToCommitEvent );
            }
        }
        }
    }

    private static void generateNextDistributedTransactionCompletionEvent( EventList eventList, Rand rand, int thisNodeId, double thisEventTime )
    {
        var nextTransactionEventTime = thisEventTime + rand.generateTransactionServiceTime();
        var nextTransactionEvent = new DistributedTransactionEvent( nextTransactionEventTime, EventType.DIST_TXN_COMPLETED, thisNodeId );
        eventList.addEvent( nextTransactionEvent );
    }
}
