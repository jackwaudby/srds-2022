package action;

import event.EventType;
import event.NodeTimeoutEvent;
import event.PrepareAckReceivedEvent;
import event.PrepareReceivedEvent;
import event.TransactionCompletionEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import state.Node;
import utils.EventList;
import utils.Rand;

import static state.Node.State.COORDINATOR;
import static state.Node.State.EXECUTING;
import static state.Node.State.FOLLOWER;

public class TransactionCompletionAction {
    private final static Logger LOGGER = Logger.getLogger(TransactionCompletionAction.class.getName());

    public static void execute(TransactionCompletionEvent event, Cluster cluster, EventList eventList, Rand rand) {
        var thisNodeId = event.getNodeId();
        var thisNode = cluster.getNode(thisNodeId);

        var thisNodeCurrentEpoch = thisNode.getCurrentEpoch();
        LOGGER.debug(String.format("    node %s has completed a transaction in epoch %s", thisNodeId, thisNodeCurrentEpoch));

        var epochTransactionStartedIn = event.getEpochTransactionStartedIn();
        if (epochTransactionStartedIn != thisNodeCurrentEpoch) {
            LOGGER.debug(String.format("    TransactionCompletionEvent for an old epoch. Current epoch: %s. Epoch associated with transaction: %s", thisNodeCurrentEpoch, epochTransactionStartedIn));
            throw new IllegalStateException("Should always wait for in-flight transactions to complete");
        }

        var thisNodeState = thisNode.getState();
        LOGGER.debug(String.format("    node %s state: %s", thisNodeId, thisNodeState));

        switch (thisNodeState) {
            case EXECUTING -> {
                handleTransactionCompletion(cluster, rand, thisNodeId, thisNode);

                var thisNodeEventTime = event.getEventTime();
                generateTransactionCompletionEvent(eventList, rand, thisNodeId, thisNodeEventTime, thisNodeCurrentEpoch);
            }

            case WAITING -> {
                if (!isNodeStillLeader(thisNodeId, thisNode)) {
                    thisNode.setState(FOLLOWER);
                    LOGGER.debug("    Transition to FOLLOWER (in-flight transaction complete and received a prepare message before epoch timed out).");

                    var leader = thisNode.getCurrentLeader();
                    var prepareAckReceivedEventTime = event.getEventTime() + rand.generateNetworkDelayDuration();
                    var prepareAckReceivedEvent = new PrepareAckReceivedEvent(
                            prepareAckReceivedEventTime,
                            EventType.PREPARE_ACK_RECEIVED,
                            thisNodeId,
                            leader,
                            thisNode.getDependencies());
                    eventList.addEvent(prepareAckReceivedEvent);
                    LOGGER.debug(String.format("   Send PREPARE_ACK_RECEIVED to current known leader: node %s at %.2fms", leader, prepareAckReceivedEventTime * 1000.0));
                } else {
                    thisNode.setState(COORDINATOR);
                    LOGGER.debug("    Transition to COORDINATOR (in-flight transaction complete and no other node tried to commit this epoch).");
                    handleTransactionCompletion(cluster, rand, thisNodeId, thisNode);

                    if (!isNodeStillLeader(thisNodeId, thisNode)) {
                        throw new IllegalStateException(String.format("Should think self is leader. Current leader: %s", thisNode.getCurrentLeader()));
                    }

                    var thisNodeKnownDependencies = thisNode.getDependencies();
                    if (!thisNodeKnownDependencies.isEmpty()) {
                        LOGGER.debug(String.format("    Send prepare message to all known dependencies: %s", thisNodeKnownDependencies));
                        for (var dependency : thisNodeKnownDependencies) {

                            var prepareReceivedEventTime = event.getEventTime() + rand.generateNetworkDelayDuration();
                            var thisNodeEpoch = thisNode.getCurrentEpoch();
                            var prepareReceivedEvent = new PrepareReceivedEvent(
                                    prepareReceivedEventTime,
                                    EventType.PREPARE_RECEIVED,
                                    thisNodeId,
                                    dependency.nodeId(),
                                    thisNodeEpoch,
                                    thisNode.getDependencies());
                            eventList.addEvent(prepareReceivedEvent);
                            LOGGER.debug(String.format("   Send PREPARE_RECEIVED to node %s at %.2fms", dependency.nodeId(), prepareReceivedEventTime * 1000.0));
                        }
                    } else {
                        LOGGER.debug("    No known dependencies. Move to next epoch, transition to EXECUTING ");
                        thisNode.setState(EXECUTING);
                        thisNode.nextEpoch();

                        var thisEventTime = event.getEventTime();
                        var thisNodeNextEpoch = thisNode.getCurrentEpoch();
                        generateTransactionCompletionEvent(eventList, rand, thisNodeId, thisEventTime, thisNodeNextEpoch);

                        var thisNodeNextTimeoutEventTime = event.getEventTime() + rand.generateNextEpochTimeout();
                        var epochEvent = new NodeTimeoutEvent(thisNodeNextTimeoutEventTime, EventType.NODE_EPOCH_TIMEOUT, thisNodeId, thisNodeNextEpoch);
                        eventList.addEvent(epochEvent);
                        LOGGER.debug(String.format("   Generate NODE_EPOCH_TIMEOUT on node %s at %.2fms", thisNodeId, thisNodeNextTimeoutEventTime * 1000.0));
                    }
                }
            }

            case FOLLOWER, COORDINATOR ->
                    throw new IllegalStateException(String.format("Should in state: %s when a transaction completes", thisNodeState));
        }
    }

    static boolean isNodeStillLeader(int thisNodeId, Node thisNode) {
        return thisNode.getCurrentLeader() == thisNodeId;
    }

    private static void handleTransactionCompletion(Cluster cluster, Rand rand, int thisNodeId, Node thisNode) {
        if (rand.isDistributedTransaction()) {
            var dependencyNodeId = rand.generateDependency(thisNodeId);
            var dependencyNode = cluster.getNode(dependencyNodeId);
            LOGGER.debug(String.format("    Distributed transaction with dependency on node %s", dependencyNodeId));

            // Assumption: if the dependency is not executing then the transaction is classed as failed
            if (dependencyNode.getState() == EXECUTING) {
                thisNode.addDependency(dependencyNodeId, dependencyNode.getCurrentEpoch());
                dependencyNode.addDependency(thisNodeId, thisNode.getCurrentEpoch());
                thisNode.incCompletedTransactions();
                LOGGER.debug(String.format("    Add a dependency on node %s", dependencyNodeId));
            } else {
                LOGGER.debug(String.format("    Ignore TransactionCompletionEvent. Dependency node %s in state: %s", dependencyNodeId, dependencyNode.getState()));
            }
        } else {
            thisNode.incCompletedTransactions();
        }
    }

    static void generateTransactionCompletionEvent(EventList eventList, Rand rand, int thisNodeId, double thisEventTime, int thisNodeCurrentEpoch) {
        double serviceTime;
        if (rand.isLongTransaction()) {
            serviceTime = rand.generateLongTransactionServiceTime();
        } else {
            serviceTime = rand.generateShortTransactionServiceTime();
        }

        var nextTransactionEventTime = thisEventTime + serviceTime;
        var nextTransactionEvent = new TransactionCompletionEvent(nextTransactionEventTime, EventType.TRANSACTION_COMPLETED, thisNodeId, thisNodeCurrentEpoch);
        eventList.addEvent(nextTransactionEvent);
        LOGGER.debug(String.format("    Generate next TransactionCompletionEvent in epoch %s on node %s at %.2fms", thisNodeCurrentEpoch, thisNodeId, nextTransactionEventTime * 1000.0));
    }
}
