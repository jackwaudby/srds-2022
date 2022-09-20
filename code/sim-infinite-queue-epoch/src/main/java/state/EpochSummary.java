package state;

import utils.Config;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class EpochSummary
{
    int epochId;
    TerminalState epochState;
    int completedTransactions;
    int abortedTransactions;
    double duration;
    double start;
    double end;
    Set<CommitGroup> commitGroups;
    Set<Cluster.Failure> failures;
    Set<Integer> repairs;
    boolean skipped;
    List<Integer> completedJobs;
    int numCommitGroupsAtFailure;

    public EpochSummary(
            int epochId,
            TerminalState epochState,
            int completedTransactions,
            int abortedTransactions,
            double duration,
            Set<CommitGroup> commitGroups,
            Set<Cluster.Failure> failures,
            Set<Integer> repairs,
            boolean skipped,
            double start,
            double end,
            List<Integer> completedJobs,
            int numCommitGroupsAtFailure
    )
    {
        this.epochId = epochId;
        this.epochState = epochState;
        this.completedTransactions = completedTransactions;
        this.abortedTransactions = abortedTransactions;
        this.duration = duration;
        this.commitGroups = commitGroups;
        this.failures = failures;
        this.repairs = repairs;
        this.skipped = skipped;
        this.start = start;
        this.end = end;
        this.completedJobs = completedJobs;
        this.numCommitGroupsAtFailure = numCommitGroupsAtFailure;
    }

    public int getEpochId()
    {

        return epochId;
    }

    public Set<Cluster.Failure> getFailures()
    {
        return failures;
    }

    public TerminalState getEpochState()
    {
        return epochState;
    }

    public double getStart()
    {
        return start;
    }

    public Set<CommitGroup> getCommitGroups()
    {
        return commitGroups;
    }

    public int getNumCommitGroupsAtFailure() {
        return numCommitGroupsAtFailure;
    }

    public int getCompletedTransactions()
    {
        return completedTransactions;
    }

    public int getAbortedTransactions()
    {
        return abortedTransactions;
    }

    @Override
    public String toString()
    {


        return "{" +
               "id=" + epochId +
               ", st=" + epochState +
               ", co=" + completedTransactions +
               ", ab=" + abortedTransactions +
               ", cg=" + commitGroups +
               ", fa=" + failures +
               ", re=" + repairs +
               ", du=" + String.format( "%.3f", duration ) +
               ", st=" + String.format( "%.3f", start ) +
               ", en=" + String.format( "%.3f", end ) +
               ", sk=" + skipped +
               ", b=" + Config.getInstance().getCommitOperationRateInSecs() +
               ", cj=" + completedJobs +
               ", di=" + failures.stream().map( f -> f.eventTime - start ).collect( Collectors.toList() ) +
               '}';
    }
}
