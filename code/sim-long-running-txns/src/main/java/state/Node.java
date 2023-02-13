package state;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Node {
    private final int id;

    private State state;

    private int currentEpoch;

    private final List<Epoch> epochs;

    public Node(int id) {
        this.id = id;
        this.state = State.EXECUTING;
        this.currentEpoch = 0;
        this.epochs = new ArrayList<>();
        epochs.add(new Epoch(id));
    }

    public enum State {
        // processing transactions
        EXECUTING,

        // timed out and waiting for local in-flight transaction to finish
        WAITING,

        // coordinating the commit of an epoch
        COORDINATOR,

        // participant in the commit of an epoch
        FOLLOWER,
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public void incCompletedTransactions() {
        epochs.get(currentEpoch).incCompletedTransactions();
    }

    public void addDependency(int depId, int depEpoch) {
        epochs.get(currentEpoch).addDependency(depId, depEpoch);
    }

    public Set<Dependency> getDependencies() {
        return epochs.get(currentEpoch).getDependencies();
    }

    public int getCurrentEpoch() {
        return currentEpoch;
    }

    public int getCurrentLeader() {
        return epochs.get(currentEpoch).getLeader();
    }

    public void setCurrentLeader(int newLeader) {
        epochs.get(currentEpoch).setLeader(newLeader);
    }

    public void nextEpoch() {
        this.currentEpoch += 1;
        this.epochs.add(currentEpoch, new Epoch(id));
    }

    public boolean isMissingDependencies(Set<Dependency> dependencies) {
        return !epochs.get(currentEpoch).getDependencies().equals(dependencies);
    }

    public void updateAcks(Set<Dependency> dependencies) {
        for (var dependency : dependencies ) {
            epochs.get(currentEpoch).getDependency(dependency.nodeId()).setAckReceived();
        }
    }

    public boolean receivedAllAcks() {
        for (var dependency : getDependencies()) {
            if (!dependency.isAckReceived()) {
                return false;
            }
        }
        return true;
    }
}
