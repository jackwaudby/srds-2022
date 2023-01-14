package event;

import java.util.Objects;

public class FailureEvent extends AbstractEvent
{
    private final int participantNodeId;

    public FailureEvent( double eventTime, EventType eventTypeEnum, int participantNodeId )
    {
        super( eventTime, eventTypeEnum );
        this.participantNodeId = participantNodeId;
    }

    public int getNodeId()
    {
        return participantNodeId;
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
        FailureEvent that = (FailureEvent) o;
        return participantNodeId == that.participantNodeId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), participantNodeId );
    }
}

