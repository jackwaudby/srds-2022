package action;

import event.CommitReceivedEvent;
import event.EventType;
import event.NodeTimeoutEvent;
import event.PrepareAckReceivedEvent;
import event.PrepareReceivedEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import state.Dependency;
import utils.EventList;
import utils.Rand;

import static action.TransactionCompletionAction.generateTransactionCompletionEvent;
import static event.EventType.COMMIT_RECEIVED;
import static state.Node.State.EXECUTING;

public class PrepareReceivedAckAction {

    private final static Logger LOGGER = Logger.getLogger(PrepareReceivedAckAction.class.getName());

    public static void prepareAckReceived(PrepareAckReceivedEvent event, Cluster cluster, Rand rand, EventList eventList) {
        var thisNodeId = event.getReceiverId();
        var thisNode = cluster.getNode(thisNodeId);
        var thisNodeState = thisNode.getState();

        LOGGER.debug(String.format("   node %s state: %s", thisNodeId, thisNodeState));
        LOGGER.debug(String.format("   %s", event.getDependencies()));
        LOGGER.debug(String.format("   from node %s", event.getSenderId()));

        if (event.getDependencies().stream().filter(dependency -> dependency.nodeId() == thisNodeId).map(Dependency::epoch).toList().get(0) < thisNode.getCurrentEpoch()) {
            //throw new IllegalStateException("Stale message");
            LOGGER.debug(String.format("   stale message from node %s", event.getSenderId()));

            return;
        }

        switch (thisNodeState) {
            case EXECUTING, WAITING -> {
                // Illegal state: Must be either FOLLOWER or COORDINATOR
                throw new IllegalStateException(String.format("Node state: %s", thisNodeState));

            }
            case FOLLOWER -> {
                // Another node has become COORDINATOR
                // Ignore this event
            }

            case COORDINATOR -> {
                // if missing any dependencies, update dependencies and send them prepare
                if (thisNode.isMissingDependencies(event.getDependencies())) {
                    LOGGER.debug("   missing dependencies");
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
                                LOGGER.debug("   send prepare to node " + expectedDependency.nodeId());

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
                    generateTransactionCompletionEvent(eventList, rand, thisNodeId, thisEventTime);

                    // next epoch
                    var nextNodeTimeoutEventTime = event.getEventTime() + rand.generateNextEpochTimeout();
                    var epochEvent = new NodeTimeoutEvent(nextNodeTimeoutEventTime, EventType.NODE_EPOCH_TIMEOUT, thisNodeId, thisNode.getCurrentEpoch());
                    eventList.addEvent(epochEvent);
                } else {
                    LOGGER.debug("   not received all acks");
                }

            }
        }
    }
}
