package state;

import utils.Config;
import utils.Metrics;

import java.util.ArrayList;
import java.util.List;

public class Cluster {

    private static final Cluster instance = new Cluster();

    double start;
    Integer currentEpoch;
    EpochState currentEpochState;
    List<Integer> completedJobs;
    List<NodeState> nodeStates;
    boolean skipped;

    private Cluster() {
        this.start = 0.0;
        this.skipped = false;
        this.currentEpoch = 0;
        this.currentEpochState = EpochState.PROCESSING;
        var clusterSize = Config.getInstance().getClusterSize();
        this.completedJobs = new ArrayList<>();

        this.nodeStates = new ArrayList<>();

        for (int i = 0; i < clusterSize; i++) {
            completedJobs.add(0);
            nodeStates.add(NodeState.OPERATIONAL);
        }
    }

    public static Cluster getInstance() {
        return instance;
    }

    public boolean isSkipped() {
        return skipped;
    }

    public void setSkipped(boolean skipped) {
        this.skipped = skipped;
    }

    public EpochState getCurrentEpochState() {
        return currentEpochState;
    }

    public Integer getCurrentEpoch() {
        return currentEpoch;
    }

    public boolean areAllOperationalNodesReadyToCommit(Config config) {
        for (int i = 0; i < config.getClusterSize(); i++) {
            var nodeState = getNodeState(i);
            if (nodeState == NodeState.OPERATIONAL) {
                return false; // one node is not ready yet
            }
        }

        return true;
    }

    public void complete(Metrics metrics, double end) {
        metrics.incCompletedEpochs();

        var duration = end - this.start;
        metrics.incCumulativeLatency(duration);
        this.start = end;

        var totalCompletedJobs = completedJobs.stream().mapToInt(a -> a).sum();
        metrics.incCompletedTransactions(totalCompletedJobs);
    }

    public void resetClusterState(Config config) {
        var clusterSize = config.getClusterSize();

        // foreach node in the cluster
        for (int node = 0; node < clusterSize; node++) {
            if (getNodeState(node) == NodeState.READY) {
                setNodeState(node, NodeState.OPERATIONAL); // TODO: not needed?
            }

            completedJobs.set(node, 0); // reset completed work
        }

        currentEpoch += 1; // increment epoch
        currentEpochState = EpochState.PROCESSING; // set to processing
    }

    public void setCurrentEpochState(EpochState currentEpochState) {
        this.currentEpochState = currentEpochState;
    }

    public void incCompletedJobs(int nodeId, int toAdd) {
        var curr = this.completedJobs.get(nodeId) + toAdd;
        this.completedJobs.set(nodeId, curr);
    }

    public void setNodeState(int nodeId, NodeState state) {
        this.nodeStates.set(nodeId, state);
    }

    public NodeState getNodeState(int nodeId) {
        return nodeStates.get(nodeId);
    }

    @Override
    public String toString() {
        return "Cluster{" +
                "start=" + start +
                ", epoch=" + currentEpoch +
                ", state=" + currentEpochState +
                ", completedJobs=" + completedJobs +
                ", nodeStates=" + nodeStates +
                '}';
    }
}
