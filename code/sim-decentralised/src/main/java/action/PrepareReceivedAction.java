package action;

import event.EventType;
import event.PrepareAckReceivedEvent;
import event.PrepareReceivedEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import state.Dependency;
import state.Node;
import utils.EventList;
import utils.Rand;

import static action.TransactionCompletionAction.isNodeStillLeader;

public class PrepareReceivedAction {
    private final static Logger LOGGER = Logger.getLogger(PrepareReceivedAction.class.getName());

    public static void prepareReceived(PrepareReceivedEvent event, Cluster cluster, Rand rand, EventList eventList) {
        var thisNodeId = event.getReceiverId();
        var thisNode = cluster.getNode(thisNodeId);
        var senderNodeId = event.getSenderId();

        LOGGER.debug(String.format("   Node %s received prepare message from node %s", thisNodeId, senderNodeId));

        var thisNodeExpectedEpoch = event.getDependencies().stream().filter(dependency -> dependency.nodeId() == thisNodeId).map(Dependency::epoch).toList().get(0);
        var thisNodeCurrentEpoch = thisNode.getCurrentEpoch();
        if (isPrepareMessageForOldEpoch(thisNodeExpectedEpoch, thisNodeCurrentEpoch)) {
            LOGGER.debug(String.format("   Ignore stale prepare message from node %s", senderNodeId));
            return;
        }

        var thisNodeState = thisNode.getState();
        LOGGER.debug(String.format("    Node %s state: %s", thisNodeId, thisNodeState));

        switch (thisNodeState) {
            case EXECUTING -> {
                thisNode.setState(Node.State.WAITING);
                LOGGER.debug("    Transition to WAITING (must complete in-flight transactions).");

                thisNode.setCurrentLeader(event.getSenderId());
                LOGGER.debug(String.format("   Update epoch leader to node %s", senderNodeId));

                updateDependencies(event, thisNodeId, thisNode);
            }
            case WAITING -> {
                var thisNodeCurrentLeader = thisNode.getCurrentLeader();
                if (shouldUpdateLeader(senderNodeId, thisNodeCurrentLeader)) {
                    thisNode.setCurrentLeader(senderNodeId);
                    LOGGER.debug(String.format("   Update epoch leader to node %s", senderNodeId));

                    updateDependencies(event, thisNodeId, thisNode);
                } else {
                    LOGGER.debug(String.format("   Ignore prepare message from node %s. Current leader is node: %s", senderNodeId, thisNodeCurrentLeader));
                }
            }
            case FOLLOWER -> {
                var thisNodeCurrentLeader = thisNode.getCurrentLeader();
                if (shouldUpdateLeader(senderNodeId, thisNodeCurrentLeader)) {
                    thisNode.setCurrentLeader(senderNodeId);
                    LOGGER.debug(String.format("   Update epoch leader to node %s", senderNodeId));

                    updateDependencies(event, thisNodeId, thisNode);

                    sendPrepareAck(event, rand, eventList, thisNodeId, thisNode);
                } else {
                    LOGGER.debug(String.format("   Ignore prepare message from node %s. Current leader is node: %s", senderNodeId, thisNodeCurrentLeader));
                }
            }
            case COORDINATOR -> {
                if (thisNodeId < senderNodeId) {
                    thisNode.setCurrentLeader(event.getSenderId());
                    LOGGER.debug(String.format("   Update epoch leader to node %s", senderNodeId));
                    thisNode.setState(Node.State.FOLLOWER);
                    LOGGER.debug("    Transition to FOLLOWER");

                    updateDependencies(event, thisNodeId, thisNode);

                    sendPrepareAck(event, rand, eventList, thisNodeId, thisNode);
                } else {
                    var thisNodeCurrentLeader = thisNode.getCurrentLeader();
                    if (!isNodeStillLeader(thisNodeId, thisNode)) {
                        throw new IllegalStateException(String.format("Should think self is leader. Current leader: %s", thisNode.getCurrentLeader()));
                    }
                    LOGGER.debug(String.format("   Ignore prepare message from node %s. Current leader is node: %s", senderNodeId, thisNodeCurrentLeader));
                }
            }
        }
    }

    private static boolean shouldUpdateLeader(int senderNodeId, int thisNodeCurrentLeader) {
        return thisNodeCurrentLeader < senderNodeId;
    }

    public static boolean isPrepareMessageForOldEpoch(Integer thisNodeExpectedEpoch, int thisNodeCurrentEpoch) {
        return thisNodeExpectedEpoch < thisNodeCurrentEpoch;
    }

    private static void updateDependencies(PrepareReceivedEvent event, int thisNodeId, Node thisNode) {
        LOGGER.debug("   Received dependencies from sender: " + event.getDependencies());
        LOGGER.debug("   Current dependencies: " + thisNode.getDependencies());

        for (var expectedDependency : event.getDependencies()) {
            if (!thisNode.getDependencies().contains(expectedDependency)) {
                // do not add self dependencies / send to self
                if (expectedDependency.nodeId() != thisNodeId) {
                    thisNode.addDependency(expectedDependency.nodeId(), expectedDependency.epoch());
                    LOGGER.debug("   Update dependencies with node " + expectedDependency.nodeId());
                }
            }
        }

        // what if sender not included
        if (!thisNode.getDependencies().contains(new Dependency(event.getSenderId(), event.getSenderEpoch()))) {
            thisNode.addDependency(event.getSenderId(), event.getSenderEpoch());
            LOGGER.debug("   Update dependencies with node " + event.getSenderId());

        }
        LOGGER.debug("   Updated dependencies: " + thisNode.getDependencies());
    }

    private static void sendPrepareAck(PrepareReceivedEvent event, Rand rand, EventList eventList, int thisNodeId, Node thisNode) {
        var leader = thisNode.getCurrentLeader();
        var prepareAckReceivedEventTime = event.getEventTime() + rand.generateNetworkDelayDuration();
        var prepareAckReceivedEvent = new PrepareAckReceivedEvent(
                prepareAckReceivedEventTime,
                EventType.PREPARE_ACK_RECEIVED,
                thisNodeId,
                leader,
                thisNode.getDependencies());
        LOGGER.debug("   Send prepare ack message to node " + leader);
        LOGGER.debug("   Sent dependencies: " + thisNode.getDependencies());

        eventList.addEvent(prepareAckReceivedEvent);
    }

}
