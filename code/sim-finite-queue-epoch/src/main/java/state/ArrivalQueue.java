package state;

import java.util.LinkedList;
import java.util.List;

public class ArrivalQueue
{
    private final LinkedList<Job> arrivals;

    private static final ArrivalQueue instance = new ArrivalQueue();

    private ArrivalQueue()
    {
        this.arrivals = new LinkedList<>();
    }

    public static ArrivalQueue getInstance()
    {
        return instance;
    }

    public void addJob( Job arrival )
    {
        arrivals.addLast( arrival );
    }

    public void addJobs( List<Job> toAdd )
    {
        arrivals.addAll( toAdd );
    }

    public boolean isQueueEmpty()
    {
        return arrivals.isEmpty();
    }

    public Job getJob()
    {
        return arrivals.removeFirst();
    }

    public int getQueueSize()
    {
        return arrivals.size();
    }

    @Override
    public String toString()
    {
        return "{" +
               arrivals +
               '}';
    }
}
