package state;

import java.util.Objects;

public class Dependency {

    private final int nodeId;
    private final int epoch;
    private boolean ackReceived;

    public Dependency(int nodeId, int epoch) {
        this.nodeId = nodeId;
        this.epoch = epoch;
        ackReceived = false;
    }

    public int nodeId() {
        return nodeId;
    }

    public int epoch() {
        return epoch;
    }

    public boolean isAckReceived() {
        return ackReceived;
    }

    public void setAckReceived() {
        this.ackReceived = true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Dependency that = (Dependency) o;
        return nodeId == that.nodeId && epoch == that.epoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, epoch);
    }

    @Override
    public String toString() {
        return "{" +
                "nodeId=" + nodeId +
                ", epoch=" + epoch +
                ", ackReceived=" + ackReceived +
                '}';
    }
}
