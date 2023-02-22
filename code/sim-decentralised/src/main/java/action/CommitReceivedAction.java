package action;

import event.CommitReceivedEvent;
import event.EventType;
import event.NodeTimeoutEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import utils.Config;
import utils.EventList;
import utils.Metrics;
import utils.Rand;

import static action.TransactionCompletionAction.generateTransactionCompletionEvent;
import static state.Node.State.EXECUTING;

/**
 * The sender of the commit message is always the coordinator for a given epoch.
 * There can never be two commit messages for the same epoch
 */
public class CommitReceivedAction {
    private final static Logger LOGGER = Logger.getLogger(CommitReceivedAction.class.getName());

    public static void commitReceived(CommitReceivedEvent event, Cluster cluster, EventList eventList, Rand rand) {
        var thisNodeId = event.getReceiverId();
        var thisNode = cluster.getNode(thisNodeId);

        var thisNodeState = thisNode.getState();
        LOGGER.debug(String.format("    Node %s state: %s", thisNodeId, thisNodeState));

        switch (thisNodeState) {
            case WAITING, EXECUTING,COORDINATOR-> {
                throw new IllegalStateException("Should be FOLLOWER to receive a commit message");
            }
            case FOLLOWER -> {
                thisNode.nextEpoch();
                thisNode.setState(EXECUTING);
                LOGGER.debug("   Transition to EXECUTING");

                var thisEventTime = event.getEventTime();
                generateTransactionCompletionEvent(eventList, rand, thisNodeId, thisEventTime, thisNode.getCurrentEpoch());

                var thisNodeNextTimeoutEventTime = event.getEventTime() + rand.generateNextEpochTimeout();
                var epochEvent = new NodeTimeoutEvent(thisNodeNextTimeoutEventTime, EventType.NODE_EPOCH_TIMEOUT, thisNodeId, thisNode.getCurrentEpoch());
                eventList.addEvent(epochEvent);
                LOGGER.debug(String.format("   Generate NODE_EPOCH_TIMEOUT on node %s at %.2fms", thisNodeId, thisNodeNextTimeoutEventTime * 1000.0));
            }
        }
    }
}

