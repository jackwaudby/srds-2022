package utils;

import event.AbortOperationEvent;
import event.CommitOperationEvent;
import event.EpochTimeoutEvent;
import event.EventType;
import event.TransactionEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import state.EpochState;
import state.NodeState;

public class Common {
    private final static Logger LOGGER = Logger.getLogger(Common.class.getName());

    public static void skipOrGenerateNextEpoch(double thisEventTime, Rand rand, EventList eventList, Cluster cluster, Config config,
                                               FailureRepairEventList failureRepairEventList) {
        var nextEpochTimeoutEvent = thisEventTime + rand.generateNextEpochTimeout();
        var currentEpoch = cluster.getCurrentEpoch();

        // generate epoch timeout event
        var epochTimeoutEvent = new EpochTimeoutEvent(nextEpochTimeoutEvent, EventType.EPOCH_TIMEOUT, currentEpoch);
        eventList.addEvent(epochTimeoutEvent);
        LOGGER.debug(String.format(" - generate next epoch timeout at %.5f (ms)", nextEpochTimeoutEvent * 1000.0));

        // generate transactions for operational nodes
        var clusterSize = config.getClusterSize();
        for (int i = 0; i < clusterSize; i++) {
            if (cluster.getNodeState(i) == NodeState.OPERATIONAL) {
                var nextTransactionCompletionTime = thisEventTime + rand.generateTransactionServiceTime();
                var transactionEvent = new TransactionEvent(nextTransactionCompletionTime, EventType.TRANSACTION_COMPLETED, i, currentEpoch);
                eventList.addEvent(transactionEvent);
            }
        }
        LOGGER.debug(" - generate transactions for operational nodes");

    }
}
