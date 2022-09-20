package event;

import java.util.Objects;

public class RepairEvent extends AbstractEvent
{
    private final int nodeId;
    private final int originEpoch;

    public RepairEvent( double eventTime, EventType eventTypeEnum, int nodeId, int originEpoch )
    {
        super( eventTime, eventTypeEnum );
        this.nodeId = nodeId;
        this.originEpoch = originEpoch;
    }

    public int getNodeId()
    {
        return nodeId;
    }

    public int getOriginEpoch()
    {
        return originEpoch;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        if ( !super.equals( o ) )
        {
            return false;
        }
        RepairEvent that = (RepairEvent) o;
        return nodeId == that.nodeId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), nodeId );
    }
}

