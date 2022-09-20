package state;

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

    public void setFailed( boolean failed )
    {
        this.failed = failed;
    }

    public int getId()
    {
        return id;
    }

    public boolean contains( int nodeId )
    {
        return this.members.contains( nodeId );
    }

    public boolean isFailed()
    {
        return failed;
    }

    public Set<Integer> getMembers()
    {
        return members;
    }

    @Override
    public String toString()
    {
        return "{" + members + ", " + failed + '}';
    }
}
