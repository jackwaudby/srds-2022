package action;

import event.*;
import state.Cluster;
import state.Node;
import utils.EventList;
import utils.Rand;

import org.apache.log4j.Logger;

import static state.Node.State.EXECUTING;
import static state.Node.State.COORDINATOR;
import static state.Node.State.FOLLOWER;

public class TransactionCompletionAction {
    private final static Logger LOGGER = Logger.getLogger(TransactionCompletionAction.class.getName());

    public static void execute(TransactionCompletionEvent event, Cluster cluster, EventList eventList, Rand rand) {
        var thisNodeId = event.getNodeId();
        var thisNode = cluster.getNode(thisNodeId);
        var thisNodeState = thisNode.getState();
        LOGGER.debug(String.format("   node %s state: %s", thisNodeId, thisNodeState));

        switch (thisNodeState) {
            case EXECUTING -> {
                handleTransactionCompletion(cluster, rand, thisNodeId, thisNode);
                generateTransactionCompletionEvent(eventList, rand, thisNodeId, event.getEventTime());
            }

            case WAITING -> {
                // if received a prepare whilst waiting to finish a txn then I won't be the leader
                if (thisNode.getCurrentLeader() != thisNodeId) {
                    thisNode.setState(FOLLOWER);

                    var leader = thisNode.getCurrentLeader();
                    var prepareAckReceivedEventTime = event.getEventTime() + rand.generateNetworkDelayDuration();
                    var prepareAckReceivedEvent = new PrepareAckReceivedEvent(
                            prepareAckReceivedEventTime,
                            EventType.PREPARE_ACK_RECEIVED,
                            thisNodeId,
                            leader,
                            thisNode.getDependencies());
                    eventList.addEvent(prepareAckReceivedEvent);
                    LOGGER.debug(String.format("   send ACK to current known leader: node %s", leader));
                } else {

                    // completed all in-flight work
                    // this means that my local epoch has timed out and nobody else has tried to commit this epoch
                    handleTransactionCompletion(cluster, rand, thisNodeId, thisNode);

                    // transition to coordinator
                    thisNode.setState(COORDINATOR);

                    if (thisNode.getCurrentLeader() != thisNodeId) {
                        throw new IllegalStateException("Should think self is leader");
                    }

                    LOGGER.debug(String.format("    %s", thisNode));
                    LOGGER.debug("    transition to COORDINATOR");

                    // send prepare message to all known dependencies
                    if (!thisNode.getDependencies().isEmpty()) {
                        LOGGER.debug("    send prepare message to all known dependencies ");
                        for (var dependency : thisNode.getDependencies()) {

                            var prepareReceivedEventTime = event.getEventTime() + rand.generateNetworkDelayDuration();
                            var thisNodeEpoch = thisNode.getCurrentEpoch();
                            var prepareReceivedEvent = new PrepareReceivedEvent(
                                    prepareReceivedEventTime,
                                    EventType.PREPARE_RECEIVED,
                                    thisNodeId,
                                    dependency.nodeId(),
                                    thisNodeEpoch,
                                    thisNode.getDependencies() );
                            eventList.addEvent(prepareReceivedEvent);
                            LOGGER.debug(String.format("    node %s at %.2f", dependency.nodeId(), prepareReceivedEventTime * 1000.0));

                        }
                    } else {
                        LOGGER.debug("    no dependencies, move to next epoch");
                        LOGGER.debug("    transition to EXECUTING");
                        thisNode.setState(EXECUTING);
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

            case FOLLOWER -> {
                // has received prepare message from another node, but haven't yet responded because was still working
                // reply with dependencies and wait for commit message
                var leader = thisNode.getCurrentLeader();
                var prepareAckReceivedEventTime = event.getEventTime() + rand.generateNetworkDelayDuration();
                var prepareAckReceivedEvent = new PrepareAckReceivedEvent(
                        prepareAckReceivedEventTime,
                        EventType.PREPARE_ACK_RECEIVED,
                        thisNodeId,
                        leader,
                        thisNode.getDependencies());
                eventList.addEvent(prepareAckReceivedEvent);
                LOGGER.debug(String.format("   send ACK to current known leader: node %s", leader));
                LOGGER.debug("   sent dependencies " + thisNode.getDependencies());

            }

            case COORDINATOR -> {
                throw new IllegalStateException(String.format("Node state: %s", thisNodeState));
            }
        }
    }

    private static void handleTransactionCompletion(Cluster cluster, Rand rand, int thisNodeId, Node thisNode) {
        if (rand.isDistributedTransaction()) {
            var dependencyNodeId = rand.generateDependency(thisNodeId);
            var dependencyNode = cluster.getNode(dependencyNodeId);

            // Assumption: if the dependency is not executing then the transaction is classed as failed
            if (dependencyNode.getState() == EXECUTING) {
                thisNode.addDependency(dependencyNodeId, dependencyNode.getCurrentEpoch());
                dependencyNode.addDependency(thisNodeId, thisNode.getCurrentEpoch());
                thisNode.incCompletedTransactions();
            }
        } else {
            thisNode.incCompletedTransactions();
        }
    }

    static void generateTransactionCompletionEvent(
            EventList eventList, Rand rand, int thisNodeId, double thisEventTime) {

        // get next transaction completion time
        double serviceTime;
        if (rand.isLongTransaction()) {
            serviceTime = rand.generateLongTransactionServiceTime();
        } else {
            serviceTime = rand.generateShortTransactionServiceTime();
        }
        var nextTransactionEventTime = thisEventTime + serviceTime;

        var nextTransactionEvent = new TransactionCompletionEvent(nextTransactionEventTime, EventType.TRANSACTION_COMPLETED, thisNodeId);
        eventList.addEvent(nextTransactionEvent);
        LOGGER.debug(String.format("    generate next transaction completion event at %.2fms", nextTransactionEventTime * 1000.0));
    }
}
