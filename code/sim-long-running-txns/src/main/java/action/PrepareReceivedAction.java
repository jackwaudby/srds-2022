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

import java.util.stream.Collectors;

public class PrepareReceivedAction {
    private final static Logger LOGGER = Logger.getLogger(PrepareReceivedAction.class.getName());

    public static void prepareReceived(PrepareReceivedEvent event, Cluster cluster, Rand rand, EventList eventList) {
        var thisNodeId = event.getReceiverId();
        var thisNode = cluster.getNode(thisNodeId);
        var thisNodeState = thisNode.getState();

        LOGGER.debug(String.format("   node %s state: %s", thisNodeId, thisNodeState));
        LOGGER.debug(String.format("   received from node %s", event.getSenderId()));

        // check for stale message
        if (event.getDependencies().stream().filter(dependency -> dependency.nodeId() == thisNodeId).map(Dependency::epoch).toList().get(0) < thisNode.getCurrentEpoch()) {
            //throw new IllegalStateException("Stale message");
            LOGGER.debug(String.format("   stale message from node %s", event.getSenderId()));

            return;
        }

        switch (thisNodeState) {
            case EXECUTING -> {
                // switch to WAITING
                thisNode.setState(Node.State.WAITING);
                LOGGER.debug(String.format("   transition to %s", Node.State.WAITING));
                // mark the sender as the leader
                thisNode.setCurrentLeader(event.getSenderId());
                LOGGER.debug(String.format("   leader set to %s", event.getSenderId()));
                // don't reply as need to wait for inflight transaction to finish

                updateDependencies(event, thisNodeId, thisNode);

            }
            case WAITING -> {
                // Either this node has timed out or has received prepare message from another node
                // update leader if the sender has higher id
                if (thisNode.getCurrentLeader() < event.getSenderId()) {
                    thisNode.setCurrentLeader(event.getSenderId());
                    LOGGER.debug(String.format("   new leader: node %s", event.getSenderId()));
//                    thisNode.setState(Node.State.FOLLOWER);
                    LOGGER.debug("   transition to FOLLOWER");

                    updateDependencies(event, thisNodeId, thisNode);

                } else {
                    LOGGER.debug("   lower id so ignore");
                }
                // don't reply as need to wait for inflight transaction to finish
            }
            case COORDINATOR -> {
                // if sender has a higher id then step down
                // mark sender as leader
                if (thisNodeId < event.getSenderId()) {
                    thisNode.setCurrentLeader(event.getSenderId());

                    // move to FOLLOWER
                    thisNode.setState(Node.State.FOLLOWER);
                    LOGGER.debug("   transition to FOLLOWER");
                    LOGGER.debug("   send ACK");

                    updateDependencies(event, thisNodeId, thisNode);

                    sendPrepareAck(event, rand, eventList, thisNodeId, thisNode);
                } else {
                    LOGGER.debug("   lower id so ignore");
                }
                // else ignore
            }
            case FOLLOWER -> {
                // if sender has a higher id than current leader then update leader
                // mark sender as leader
                // send prepare ack
                if (thisNode.getCurrentLeader() < event.getSenderId()) {

                    updateDependencies(event, thisNodeId, thisNode);

                    thisNode.setCurrentLeader(event.getSenderId());
                    sendPrepareAck(event, rand, eventList, thisNodeId, thisNode);
                }
                // else ignore
            }
        }


    }

    private static void updateDependencies(PrepareReceivedEvent event, int thisNodeId, Node thisNode) {
        LOGGER.debug("   received dependencies from sender: " + event.getDependencies());
        LOGGER.debug("   current dependencies: " + thisNode.getDependencies());

        for (var expectedDependency : event.getDependencies()) {
            if (!thisNode.getDependencies().contains(expectedDependency)) {
                // do not add self dependencies / send to self
                if (expectedDependency.nodeId() != thisNodeId) {
                    thisNode.addDependency(expectedDependency.nodeId(), expectedDependency.epoch());
                    LOGGER.debug("   update dependencies with node " + expectedDependency.nodeId());
                }
            }
        }
        LOGGER.debug("   updated dependencies: " + thisNode.getDependencies());

        // what if sender not included
        if (!thisNode.getDependencies().contains(new Dependency(event.getSenderId(), event.getSenderEpoch()))) {
//            throw new IllegalStateException("Missing sender as a dependency");
            thisNode.addDependency(event.getSenderId(), event.getSenderEpoch());
            LOGGER.debug("   update dependencies with node " + event.getSenderId());

        }
        LOGGER.debug("   updated dependencies: " + thisNode.getDependencies());


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
        LOGGER.debug("   send prepare ack to node " + leader);
        LOGGER.debug("   sent dependencies " + thisNode.getDependencies());

        eventList.addEvent(prepareAckReceivedEvent);
    }

}
