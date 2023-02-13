package action;

import event.CommitReceivedEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import utils.Config;
import utils.EventList;
import utils.Metrics;
import utils.Rand;

/**
 * The sender of the commit message is always the coordinator for a given epoch.
 * There can never be two commit messages for the same epoch
 */
public class CommitReceivedAction {
    private final static Logger LOGGER = Logger.getLogger(CommitReceivedAction.class.getName());

    public static void commitReceived(CommitReceivedEvent event, Cluster cluster, Config config, EventList eventList, Rand rand, Metrics metrics) {
        var thisNodeId = event.getReceiverId();
        var node = cluster.getNode(thisNodeId);
        var nodeState = node.getState();

        switch (nodeState) {
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
            }

            case COORDINATOR -> {
                // Illegal state: Must be either FOLLOWER or COORDINATOR
                // To have replied with ack must be FOLLOWER
            }
        }
    }
}

