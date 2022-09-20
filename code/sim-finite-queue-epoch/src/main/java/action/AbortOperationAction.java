package action;

import event.AbortOperationEvent;
import state.ArrivalQueue;
import state.Cluster;
import utils.Common;
import utils.Config;
import utils.EventList;
import utils.Metrics;
import utils.Rand;
import org.apache.log4j.Logger;

public class AbortOperationAction
{
    private final static Logger LOGGER = Logger.getLogger( AbortOperationAction.class.getName() );

    public static void abort( AbortOperationEvent event, Cluster cluster, Config config, EventList eventList, Rand rand, Metrics metrics, ArrivalQueue queue )
    {
        var thisEventTime = event.getEventTime();

        // inc failed epochs, record operational commit groups, record cumulative latency, move completed jobs to arrivals, compute job totals
        cluster.totalFailure( config, metrics, thisEventTime, queue );

        // switch back to idle, completed jobs, lost jobs, job stack
        cluster.resetClusterState( config, metrics );

        // generate next epoch timeout
        Common.generateNextEpochTimeoutEvent( thisEventTime, rand, eventList, cluster, config, queue, metrics );
    }
}
