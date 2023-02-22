package state;

import java.util.List;
import java.util.Set;

public class CommitGroup
{
    int id;
    Set<Integer> members;
    boolean failed;

    public CommitGroup( int id, Set<Integer> members )
    {
        this.id = id;
        this.members = members;
        this.failed = false;
    }

    public List<Integer> getMembers()
    {
        return members.stream().toList();
    }

    @Override
    public String toString()
    {
        return "{" + members + ", " + failed + '}';
    }
}
