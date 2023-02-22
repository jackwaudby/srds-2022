package action;

import event.CommitReceivedEvent;
import event.EventType;
import event.NodeTimeoutEvent;
import event.PrepareAckReceivedEvent;
import event.PrepareReceivedEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import state.Dependency;
import state.Node;
import utils.EventList;
import utils.Rand;

import static action.PrepareReceivedAction.isPrepareMessageForOldEpoch;
import static action.TransactionCompletionAction.generateTransactionCompletionEvent;
import static event.EventType.COMMIT_RECEIVED;
import static state.Node.State.EXECUTING;
import static state.Node.State.FOLLOWER;

public class PrepareReceivedAckAction {

    private final static Logger LOGGER = Logger.getLogger(PrepareReceivedAckAction.class.getName());

    public static void prepareAckReceived(PrepareAckReceivedEvent event, Cluster cluster, Rand rand, EventList eventList) {
        var thisNodeId = event.getReceiverId();
        var thisNode = cluster.getNode(thisNodeId);
        var senderNodeId = event.getSenderId();

        if (isStaleMessage(event, thisNodeId, thisNode, senderNodeId)) return;

        var thisNodeState = thisNode.getState();
        LOGGER.debug(String.format("    Node %s state: %s", thisNodeId, thisNodeState));

        switch (thisNodeState) {
            case EXECUTING, WAITING ->
                    throw new IllegalStateException("Should be FOLLOWER or COORDINATOR to receive a prepare ack message");
            case FOLLOWER -> LOGGER.debug(String.format("   Ignore prepare ack message. Another node has %s has become leader", senderNodeId));

            case COORDINATOR -> {
                // if missing any dependencies, update dependencies and send them prepare
                if (thisNode.isMissingDependencies(event.getDependencies())) {
                    LOGGER.debug("   Missing dependencies");
                    for (var expectedDependency : event.getDependencies()) {
                        if (!thisNode.getDependencies().contains(expectedDependency)) {
                            // do not add self dependencies / send to self
                            if (expectedDependency.nodeId() != thisNodeId) {
                                thisNode.addDependency(expectedDependency.nodeId(), expectedDependency.epoch());

                                var prepareReceivedEventTime = event.getEventTime() + rand.generateNetworkDelayDuration();
                                var thisNodeEpoch = thisNode.getCurrentEpoch();
                                var prepareReceivedEvent = new PrepareReceivedEvent(
                                        prepareReceivedEventTime,
                                        EventType.PREPARE_RECEIVED,
                                        thisNodeId,
                                        expectedDependency.nodeId(),
                                        thisNodeEpoch, thisNode.getDependencies());
                                eventList.addEvent(prepareReceivedEvent);
                                LOGGER.debug("   Send prepare to node " + expectedDependency.nodeId());
                            }
                        }
                    }
                }

                thisNode.updateAcks(event.getSenderId());

                LOGGER.debug("   ack received from node " + event.getSenderId());

                // If received all acks then send commit message and begin next epoch
                if (thisNode.receivedAllAcks()) {

                    LOGGER.debug("   received all acks");

                    for (var dependency : thisNode.getDependencies()) {
                        var commitReceivedEventTime = event.getEventTime() + rand.generateNetworkDelayDuration();
                        var commitReceivedEvent = new CommitReceivedEvent(
                                commitReceivedEventTime,
                                COMMIT_RECEIVED,
                                thisNodeId,
                                dependency.nodeId(),
                                thisNode.getCurrentEpoch());
                        eventList.addEvent(commitReceivedEvent);
                    }

                    thisNode.nextEpoch();
                    thisNode.setState(EXECUTING);
                    LOGGER.debug("   transition to EXECUTING");

                    // next transaction completion
                    var thisEventTime = event.getEventTime();
                    generateTransactionCompletionEvent(eventList, rand, thisNodeId, thisEventTime, thisNode.getCurrentEpoch());

                    var thisNodeNextTimeoutEventTime = event.getEventTime() + rand.generateNextEpochTimeout();
                    var epochEvent = new NodeTimeoutEvent(thisNodeNextTimeoutEventTime, EventType.NODE_EPOCH_TIMEOUT, thisNodeId, thisNode.getCurrentEpoch());
                    eventList.addEvent(epochEvent);
                    LOGGER.debug(String.format("   Generate NODE_EPOCH_TIMEOUT on node %s at %.2fms", thisNodeId, thisNodeNextTimeoutEventTime * 1000.0));
                } else {
                    LOGGER.debug("   Not received all acks");
                }

            }
        }
    }

    private static boolean isStaleMessage(PrepareAckReceivedEvent event, int thisNodeId, Node thisNode, int senderNodeId) {
        var thisNodeExpectedEpoch = event.getDependencies().stream().filter(dependency -> dependency.nodeId() == thisNodeId).map(Dependency::epoch).toList().get(0);
        var thisNodeCurrentEpoch = thisNode.getCurrentEpoch();
        if (isPrepareMessageForOldEpoch(thisNodeExpectedEpoch, thisNodeCurrentEpoch)) {
            LOGGER.debug(String.format("   Ignore stale prepare message from node %s", senderNodeId));
            return true;
        }
        return false;
    }
}
