package state;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Epoch {
    private int completedTransactions;
    private int leader;
    private final Map<Integer, Dependency> dependencies;


    public Epoch(int leader) {
        this.leader = leader;
        this.dependencies = new HashMap<>();
    }

    public void incCompletedTransactions() {
        completedTransactions += 1;
    }

    public int getCompletedTransactions() {
        return completedTransactions;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int newLeader) {
        this.leader = newLeader;
    }

    public void addDependency(int depId, int depEpoch) {
        if (dependencies.containsKey(depId)) {
            var dependency = dependencies.get(depId);
            if (depEpoch != dependency.epoch()) {
                throw new IllegalStateException("Dependencies do not match ");
            }

        } else {
            dependencies.put(depId, new Dependency(depId, depEpoch));
        }
    }

    public Dependency getDependency(int depId) {
        return dependencies.get(depId);
    }

    public Set<Dependency> getDependencies() {
        return new HashSet<>(dependencies.values());
    }
}
