package action;

import event.EpochTimeoutEvent;
import event.EventType;
import event.ReadyToCommitEvent;
import event.DistributedTransactionEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import utils.Config;
import utils.EventList;
import utils.Metrics;
import utils.Rand;

public class ReadyToCommitAction
{
    private final static Logger LOGGER = Logger.getLogger( ReadyToCommitAction.class.getName() );

    public static void ready( ReadyToCommitEvent event, Cluster cluster, Config config, EventList eventList, Rand rand )
    {
        var thisEventTime = event.getEventTime();

        cluster.computeCommitGroups();
        cluster.complete( Metrics.getInstance() );
        cluster.resetClusterState( config );

        generateNextEpochTimeoutEvent( eventList, rand, thisEventTime );
        generateNextDistributedTransactionCompletionEvents( config, eventList, rand, thisEventTime );
    }

    private static void generateNextEpochTimeoutEvent( EventList eventList, Rand rand, double thisEventTime )
    {
        var nextEpochTimeoutEvent = thisEventTime + rand.generateNextEpoch();
        var epochTimeoutEvent = new EpochTimeoutEvent( nextEpochTimeoutEvent, EventType.EPOCH_TIMEOUT );
        eventList.addEvent( epochTimeoutEvent );
    }

    private static void generateNextDistributedTransactionCompletionEvents( Config config, EventList eventList, Rand rand, double thisEventTime )
    {
        var clusterSize = config.getClusterSize();
        for ( int nodeId = 0; nodeId < clusterSize; nodeId++ )
        {
            var nextTransactionCompletionTime = thisEventTime + rand.generateTransactionServiceTime();
            var transactionEvent = new DistributedTransactionEvent( nextTransactionCompletionTime, EventType.DIST_TXN_COMPLETED, nodeId );
            eventList.addEvent( transactionEvent );
        }
    }
}
