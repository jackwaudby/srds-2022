package action;

import event.CommitOperationEvent;
import state.Cluster;
import state.NodeState;
import utils.Common;
import utils.Config;
import utils.EventList;
import utils.FailureRepairEventList;
import utils.Metrics;
import utils.Rand;

import java.util.Objects;

public class CommitOperationAction {
    public static void commit(CommitOperationEvent event, Cluster cluster, Config config, EventList eventList, Rand rand, Metrics metrics) {
        var currentEpoch = cluster.getCurrentEpoch();
        var originEpoch = event.getEpoch();
        if (originEpoch < currentEpoch) {
            return;
        }

        var thisEventTime = event.getEventTime();

        switch (cluster.getCurrentEpochState()) {
            case PROCESSING -> {
                throw new IllegalStateException("Should not be in this state");
            }
            case COMMITTING -> {
                cluster.complete(metrics, thisEventTime);
                cluster.resetClusterState(config);
                Common.generateNextEpoch(thisEventTime, rand, eventList, cluster, config);
            }
            case ABORTING, WAITING -> {
            }
        }
    }
}

