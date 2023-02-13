package state;

import utils.Config;
import utils.Metrics;

import java.util.ArrayList;
import java.util.List;

public class Cluster {
    private static final Cluster instance = new Cluster();
    double start;
    List<Node> nodes;

    private Cluster() {
        this.start = 0.0;

        var clusterSize = Config.getInstance().getClusterSize();
        this.nodes = new ArrayList<>();
        for (int i = 0; i < clusterSize; i++) {
            nodes.add(new Node(i));
        }
    }

    public static Cluster getInstance() {
        return instance;
    }

//     public void complete(Metrics metrics, double end) {
//        metrics.incCompletedEpochs();
//
//        var duration = end - this.start;
//        metrics.incCumulativeLatency(duration);
//        this.start = end;
//
//        var totalCompletedJobs = completedJobs.stream().mapToInt(a -> a).sum();
//        metrics.incCompletedTransactions(totalCompletedJobs);
//    }
//
//    public void resetClusterState(Config config) {
//        var clusterSize = config.getClusterSize();
//
//        // foreach node in the cluster
//        for (int node = 0; node < clusterSize; node++) {
//            if (getNode(node) == NodeState.READY) {
//                setNodeState(node, NodeState.OPERATIONAL); // TODO: not needed?
//            }
//
//            completedJobs.set(node, 0); // reset completed work
//        }
//
//        currentEpoch += 1; // increment epoch
//        currentEpochState = EpochState.PROCESSING; // set to processing
//    }
//
//    public void setCurrentEpochState(EpochState currentEpochState) {
//        this.currentEpochState = currentEpochState;
//    }
//
//    public void incCompletedJobs(int nodeId, int toAdd) {
//        var curr = this.completedJobs.get(nodeId) + toAdd;
//        this.completedJobs.set(nodeId, curr);
//    }
//
//    public void setNodeState(int nodeId, NodeState state) {
//        this.nodeStates.set(nodeId, state);
//    }

    public Node getNode(int nodeId) {
        return nodes.get(nodeId);
    }
}
