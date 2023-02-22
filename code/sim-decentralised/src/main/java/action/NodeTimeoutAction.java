package action;

import event.NodeTimeoutEvent;
import org.apache.log4j.Logger;
import state.Cluster;

import static state.Node.State.WAITING;

public class NodeTimeoutAction {

    private final static Logger LOGGER = Logger.getLogger(NodeTimeoutAction.class.getName());

    public static void timeout(NodeTimeoutEvent event, Cluster cluster) {
        var thisNodeId = event.getNodeId();
        var thisNode = cluster.getNode(thisNodeId);
        var thisNodeCurrentEpoch = thisNode.getCurrentEpoch();
        LOGGER.debug(String.format("    epoch %s on node %s has timed out", thisNodeCurrentEpoch, thisNodeId));

        var epochAssociatedWithTimeout = event.getEpoch();
        if (epochAssociatedWithTimeout != thisNodeCurrentEpoch) {
            LOGGER.debug(String.format("    Ignore NodeTimeoutEvent for an old epoch. Current epoch: %s. Epoch associated with timeout: %s", thisNodeCurrentEpoch, epochAssociatedWithTimeout));
            return;
        }

        var thisNodeState = thisNode.getState();
        LOGGER.debug(String.format("    node %s state: %s", thisNodeId, thisNodeState));

        switch (thisNodeState) {
            case EXECUTING -> {
                thisNode.setState(WAITING);
                LOGGER.debug("    Transition to WAITING (must complete in-flight transactions).");
                var thisNodeKnownDependencies = thisNode.getDependencies();
                LOGGER.debug(String.format("    Current leader in epoch %s: %s", thisNodeCurrentEpoch, thisNode.getCurrentLeader()));
                LOGGER.debug(String.format("    Detected dependencies in epoch %s: %s", thisNodeCurrentEpoch, thisNodeKnownDependencies));
            }
            case WAITING -> LOGGER.debug("   No action taken. Already in WAITING state (must have received a prepare message).");
            case FOLLOWER -> LOGGER.debug("   No action taken. Already in WAITING state (must have received a prepare message and in-flight transaction completed).");
            case COORDINATOR -> throw new IllegalStateException(String.format("Can't transition from %s to %s within an epoch", WAITING, thisNodeState));
        }
    }
}
