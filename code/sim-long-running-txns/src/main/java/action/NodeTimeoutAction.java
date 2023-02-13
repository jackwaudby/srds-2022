package action;

import event.NodeTimeoutEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import utils.EventList;
import utils.Rand;

import static state.Node.State.WAITING;

public class NodeTimeoutAction {

    private final static Logger LOGGER = Logger.getLogger(NodeTimeoutAction.class.getName());

    public static void timeout(NodeTimeoutEvent event, Cluster cluster, Rand rand, EventList eventList) {
        var thisNodeId = event.getNodeId();
        var node = cluster.getNode(thisNodeId);
        var nodeState = node.getState();

        switch (nodeState) {
            case EXECUTING -> {
                // Transition to WAITING
                // Wait for the node to complete its in-flight transaction
                node.setState(WAITING);
                LOGGER.debug("    transition to READY");
            }

            case WAITING -> {
                // Already received prepare message
                // Do nothing -- ignore local timeout
            }

            case FOLLOWER -> {
                // Already received prepare message and in-flight transaction has finished
                // Do nothing -- ignore local timeout
            }

            case COORDINATOR -> {
                // Illegal transition EXECUTING -> COORDINATOR: Must transition through WAITING
                throw new IllegalStateException(String.format("Node state: %s", nodeState));
            }
        }
    }
}
