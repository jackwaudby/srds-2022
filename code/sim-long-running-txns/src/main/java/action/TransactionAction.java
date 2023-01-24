package action;

import event.EventType;
import event.ReadyToCommitEvent;
import event.TransactionEvent;
import state.Cluster;
import state.NodeState;
import utils.Config;
import utils.EventList;
import utils.Rand;

public class TransactionAction {
    public static void execute(TransactionEvent event, Cluster cluster, Config config, EventList eventList, Rand rand) {
        // transaction has completed after its epoch has terminated -- ignore // TODO: problem?
        var currentEpoch = cluster.getCurrentEpoch();
        var originEpoch = event.getEpoch();
        var thisNodeId = event.getNodeId();

        if (originEpoch < currentEpoch) {
            return;
        }

        var thisEventTime = event.getEventTime();

        switch (cluster.getCurrentEpochState()) {
            // job completes whilst the epoch is processing
            case PROCESSING -> {
                if (cluster.getNodeState(thisNodeId) == NodeState.OPERATIONAL) {
                    cluster.incCompletedJobs(thisNodeId, 1);
                }

                // next transaction completion
                generateTransactionCompletionEvent(eventList, rand, currentEpoch, thisNodeId, thisEventTime);
            }
            // the epoch has timed out and now waiting for jobs to finish
            case WAITING -> {
                // all operational nodes must have finished executing their current transaction before a commit operation event is generated
                cluster.setNodeState(thisNodeId, NodeState.READY);
                cluster.incCompletedJobs(thisNodeId, 1);
                // if all operational nodes are commit then generate a commit completed operation event
                if (cluster.areAllOperationalNodesReadyToCommit(config)) {
                    eventList.addEvent(new ReadyToCommitEvent(thisEventTime, EventType.READY_TO_COMMIT, currentEpoch));
                }
            }
            case COMMITTING, ABORTING -> {
            }
        }
    }

    private static void generateTransactionCompletionEvent(EventList eventList, Rand rand, int currentEpoch, int thisNodeId, double thisEventTime) {
        double serviceTime;
        if (rand.isLongTransaction()) {
            serviceTime = rand.generateLongTransactionServiceTime();
        } else {
            serviceTime = rand.generateShortTransactionServiceTime();
        }

        var nextTransactionEventTime = thisEventTime + serviceTime;


        var nextTransactionEvent = new TransactionEvent(nextTransactionEventTime, EventType.TRANSACTION_COMPLETED, thisNodeId, currentEpoch);
        eventList.addEvent(nextTransactionEvent);
    }
}
