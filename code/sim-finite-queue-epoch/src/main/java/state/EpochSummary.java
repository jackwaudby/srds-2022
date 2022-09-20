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
    List<Integer> completedJobs;
    double cumRespTime;

    public EpochSummary(
            int epochId,
            TerminalState epochState,
            int completedTransactions,
            int abortedTransactions,
            double duration,
            Set<CommitGroup> commitGroups,
            Set<Cluster.Failure> failures,
            Set<Integer> repairs,
            double start,
            double end,
            List<Integer> completedJobs,
            double cumRespTime
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
        this.start = start;
        this.end = end;
        this.completedJobs = completedJobs;
        this.cumRespTime = cumRespTime;
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
               epochId +
               ", " + epochState +
               ", " + completedTransactions +
               ", " + abortedTransactions +
               ", " + failures +
               ", " + repairs +
               ", " + String.format( "%.1f", duration * 1000 ) +
               ", " + cumRespTime +
               '}';
    }
}
