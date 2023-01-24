package action;

import event.AbortOperationEvent;
import state.Cluster;
import utils.Common;
import utils.Config;
import utils.EventList;
import utils.FailureRepairEventList;
import utils.Metrics;
import utils.Rand;

public class AbortOperationAction
{
    public static void abort( AbortOperationEvent event, Cluster cluster, Config config, EventList eventList, Rand rand, Metrics metrics,
                              FailureRepairEventList failureRepairEventList )
    {
        // an abort implies a total failure of an epoch
        var thisEventTime = event.getEventTime();

        // record latency, lost jobs, and summary of epoch

        // reset 2PC ready flags, completed jobs, and lost jobs
        cluster.resetClusterState( config);

        // skip or generate next epoch
        Common.skipOrGenerateNextEpoch( thisEventTime, rand, eventList, cluster, config, failureRepairEventList );
    }
}
