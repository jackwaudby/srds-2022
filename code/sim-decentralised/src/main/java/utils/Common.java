package utils;

import event.NodeTimeoutEvent;
import event.EventType;
import event.TransactionCompletionEvent;
import org.apache.log4j.Logger;
import state.Cluster;

public class Common {
    private final static Logger LOGGER = Logger.getLogger(Common.class.getName());

//    public static void generateNextEpoch(double thisEventTime, Rand rand, EventList eventList, Cluster cluster, Config config) {
//        var nextEpochTimeoutEvent = thisEventTime + rand.generateNextEpochTimeout();
//        var currentEpoch = cluster.getCurrentEpoch();
//
//        // generate epoch timeout event
//        var epochTimeoutEvent = new NodeTimeoutEvent(nextEpochTimeoutEvent, EventType.NODE_EPOCH_TIMEOUT, nodeId, currentEpoch);
//        eventList.addEvent(epochTimeoutEvent);
//        LOGGER.debug(String.format(" - generate next epoch timeout at %.5f (ms)", nextEpochTimeoutEvent * 1000.0));
//
//        // generate transactions for operational nodes
//        var clusterSize = config.getClusterSize();
//        for (int i = 0; i < clusterSize; i++) {
//            if (cluster.getNode(i) == NodeState.OPERATIONAL) {
//                var nextTransactionCompletionTime = thisEventTime + rand.generateShortTransactionServiceTime();
//                var transactionEvent = new TransactionCompletionEvent(nextTransactionCompletionTime, EventType.TRANSACTION_COMPLETED, i);
//                eventList.addEvent(transactionEvent);
//            }
//        }
//        LOGGER.debug(" - generate transactions for operational nodes");
//
//    }
}
