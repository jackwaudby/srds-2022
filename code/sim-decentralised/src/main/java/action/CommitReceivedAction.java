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

    public static void commitReceived(CommitReceivedEvent event, Cluster cluster, Config config, EventList eventList, Rand rand, Metrics metrics) {
        var thisNodeId = event.getReceiverId();
        var thisNode = cluster.getNode(thisNodeId);
        var thisNodeState = thisNode.getState();
        LOGGER.debug(String.format("   node %s state: %s", thisNodeId, thisNodeState));

        switch (thisNodeState) {
            case EXECUTING -> {
                // Illegal state: Must be either FOLLOWER or COORDINATOR
                // To have replied with ack must be FOLLOWER

            }

            case WAITING -> {
                // Illegal state: Must be either FOLLOWER or COORDINATOR
                // To have replied with ack must be FOLLOWER
            }

            case FOLLOWER -> {
                // Start next epoch
                thisNode.nextEpoch();
                thisNode.setState(EXECUTING);

                // next transaction completion
                var thisEventTime = event.getEventTime();
                generateTransactionCompletionEvent(eventList, rand, thisNodeId, thisEventTime);

                // next epoch
                var nextNodeTimeoutEventTime = event.getEventTime() + rand.generateNextEpochTimeout();
                var epochEvent = new NodeTimeoutEvent(nextNodeTimeoutEventTime, EventType.NODE_EPOCH_TIMEOUT, thisNodeId, thisNode.getCurrentEpoch());
                eventList.addEvent(epochEvent);
            }

            case COORDINATOR -> {
                // Illegal state: Must be either FOLLOWER or COORDINATOR
                // To have replied with ack must be FOLLOWER
            }
        }
    }
}

