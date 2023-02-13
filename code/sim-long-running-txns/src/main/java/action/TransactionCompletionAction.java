package action;

import event.*;
import state.Cluster;
import state.Node;
import utils.Config;
import utils.EventList;
import utils.Rand;

import org.apache.log4j.Logger;

import static state.Node.State.EXECUTING;
import static state.Node.State.COORDINATOR;

public class TransactionCompletionAction {
    private final static Logger LOGGER = Logger.getLogger(TransactionCompletionAction.class.getName());

    public static void execute(TransactionCompletionEvent event, Cluster cluster, Config config, EventList eventList, Rand rand) {
        var thisNodeId = event.getNodeId();
        var thisNode = cluster.getNode(thisNodeId);
        var thisNodeState = thisNode.getState();

        switch (thisNodeState) {
            case EXECUTING -> {
                handleTransactionCompletion(cluster, rand, thisNodeId, thisNode);

                // next transaction completion
                var thisEventTime = event.getEventTime();
                generateTransactionCompletionEvent(eventList, rand, thisNodeId, thisEventTime);
            }

            case WAITING -> {
                // completed all in-flight work
                // this means that my local epoch has timed out and nobody else has tried to commit this epoch
                handleTransactionCompletion(cluster, rand, thisNodeId, thisNode);

                // transition to coordinator
                thisNode.setState(COORDINATOR);
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
                                thisNodeEpoch);
                        eventList.addEvent(prepareReceivedEvent);
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

            case FOLLOWER -> {
                // has received prepare message from another node, but haven't yet responded because was still working
                // reply with dependencies and wait for commit message
                var leader = thisNode.getCurrentLeader();
                var prepareAckReceivedEventTime =  event.getEventTime() + rand.generateNetworkDelayDuration();
                var prepareAckReceivedEvent = new PrepareAckReceivedEvent(
                        prepareAckReceivedEventTime,
                        EventType.PREPARE_ACK_RECEIVED,
                        thisNodeId,
                        leader,
                        thisNode.getDependencies());
                eventList.addEvent(prepareAckReceivedEvent);
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
