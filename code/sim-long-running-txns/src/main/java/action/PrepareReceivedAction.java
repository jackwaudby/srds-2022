package action;

import event.EventType;
import event.PrepareAckReceivedEvent;
import event.PrepareReceivedEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import state.Node;
import utils.EventList;
import utils.Rand;

public class PrepareReceivedAction {
    private final static Logger LOGGER = Logger.getLogger(PrepareReceivedAction.class.getName());

    public static void prepareReceived(PrepareReceivedEvent event, Cluster cluster, Rand rand, EventList eventList) {
        var thisNodeId = event.getReceiverId();
        var thisNode = cluster.getNode(thisNodeId);
        var thisNodeState = thisNode.getState();

        LOGGER.debug(String.format("   node %s state: %s", thisNodeId, thisNodeState));
        LOGGER.debug(String.format("   received from node %s", event.getSenderId()));

        switch (thisNodeState) {
            case EXECUTING -> {
                // switch to WAITING
                thisNode.setState(Node.State.WAITING);
                // mark the sender as the leader
                thisNode.setCurrentLeader(event.getSenderId());
                // don't reply as need to wait for inflight transaction to finish
            }
            case WAITING -> {
                // Either this node has timed out or has received prepare message from another node
                // update leader if the sender has higher id
                if (thisNode.getCurrentLeader() < event.getSenderId()) {
                    thisNode.setCurrentLeader(event.getSenderId());
                    LOGGER.debug(String.format("   new leader: node %s", event.getSenderId()));
                    thisNode.setState(Node.State.FOLLOWER);
                    LOGGER.debug("   transition to FOLLOWER");
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

                    sendPrepareAck(event, rand, eventList, thisNodeId, thisNode);
                }
                // else ignore
            }
            case FOLLOWER -> {
                // if sender has a higher id than current leader then update leader
                // mark sender as leader
                // send prepare ack
                if (thisNode.getCurrentLeader() < event.getSenderId()) {
                    thisNode.setCurrentLeader(event.getSenderId());
                    sendPrepareAck(event, rand, eventList, thisNodeId, thisNode);
                }
                // else ignore
            }
        }


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
        eventList.addEvent(prepareAckReceivedEvent);
    }

}
