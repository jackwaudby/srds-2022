package action;

import event.CommitReceivedEvent;
import event.EventType;
import event.NodeTimeoutEvent;
import event.PrepareAckReceivedEvent;
import event.PrepareReceivedEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import utils.EventList;
import utils.Rand;

import static action.TransactionCompletionAction.generateTransactionCompletionEvent;
import static event.EventType.COMMIT_RECEIVED;
import static state.Node.State.WAITING;

public class PrepareReceivedAckAction {

    private final static Logger LOGGER = Logger.getLogger(PrepareReceivedAckAction.class.getName());

    public static void prepareAckReceived(PrepareAckReceivedEvent event, Cluster cluster, Rand rand, EventList eventList) {
        var thisNodeId = event.getReceiverId();
        var thisNode = cluster.getNode(thisNodeId);
        var thisNodeState = thisNode.getState();

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
                    for (var expectedDependency : event.getDependencies()) {
                        if (!thisNode.getDependencies().contains(expectedDependency)) {
                            var prepareReceivedEventTime = event.getEventTime() + rand.generateNetworkDelayDuration();
                            var thisNodeEpoch = thisNode.getCurrentEpoch();
                            var prepareReceivedEvent = new PrepareReceivedEvent(
                                    prepareReceivedEventTime,
                                    EventType.PREPARE_RECEIVED,
                                    thisNodeId,
                                    expectedDependency.nodeId(),
                                    thisNodeEpoch);
                            eventList.addEvent(prepareReceivedEvent);
                            thisNode.addDependency(expectedDependency.nodeId(), expectedDependency.epoch());
                        }
                    }
                }

                thisNode.updateAcks(event.getDependencies());

                // If received all acks then send commit message and begin next epoch
                if (thisNode.receivedAllAcks()) {
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

                    // next transaction completion
                    var thisEventTime = event.getEventTime();
                    generateTransactionCompletionEvent(eventList, rand, thisNodeId, thisEventTime);

                    // next epoch
                    var nextNodeTimeoutEventTime = event.getEventTime() + rand.generateNextEpochTimeout();
                    var epochEvent = new NodeTimeoutEvent(nextNodeTimeoutEventTime, EventType.NODE_EPOCH_TIMEOUT, thisNodeId, thisNode.getCurrentEpoch());
                    eventList.addEvent(epochEvent);
                }
            }
        }
    }
}
