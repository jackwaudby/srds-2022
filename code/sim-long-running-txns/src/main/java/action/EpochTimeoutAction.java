package action;

import event.EpochTimeoutEvent;

import state.Cluster;
import state.EpochState;
import state.NodeState;
import utils.Common;
import utils.Config;
import utils.EventList;
import utils.Rand;

import java.util.Objects;

public class EpochTimeoutAction {
    public static void timeout(Cluster cluster) {
        cluster.setCurrentEpochState(EpochState.WAITING);

    }
}
