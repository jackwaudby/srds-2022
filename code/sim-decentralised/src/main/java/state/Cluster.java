package state;

import utils.Config;

import java.util.ArrayList;
import java.util.List;

public class Cluster {
    private static final Cluster instance = new Cluster();
    double stop;

    List<Node> nodes;

    private Cluster() {
        this.stop = 0.0;

        var clusterSize = Config.getInstance().getClusterSize();
        this.nodes = new ArrayList<>();
        for (int i = 0; i < clusterSize; i++) {
            nodes.add(new Node(i));
        }
    }

    public static Cluster getInstance() {
        return instance;
    }

    public Node getNode(int nodeId) {
        return nodes.get(nodeId);
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public double getStop() {
        return stop;
    }

    public void setStop(double stop) {
        this.stop = stop;
    }
}
