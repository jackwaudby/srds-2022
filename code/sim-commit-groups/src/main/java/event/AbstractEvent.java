package event;

import java.util.Objects;

public abstract class AbstractEvent implements Event, Comparable<AbstractEvent>
{

    private final double eventTime;           // event time
    private final EventType eventTypeEnum;        // event type

    AbstractEvent( double eventTime, EventType eventTypeEnum )
    {
        this.eventTime = eventTime;
        this.eventTypeEnum = eventTypeEnum;
    }

    @Override
    public double getEventTime()
    {
        return eventTime;
    }

    @Override
    public EventType getEventType()
    {
        return eventTypeEnum;
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
        AbstractEvent that = (AbstractEvent) o;
        return Double.compare( that.eventTime, eventTime ) == 0 && eventTypeEnum == that.eventTypeEnum;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( eventTime, eventTypeEnum );
    }

    @Override
    public int compareTo( AbstractEvent event )
    {
        int result;
        if ( (this.eventTime - event.getEventTime()) < 0 )
        {
            result = -1;
        }
        else if ( (this.eventTime - event.getEventTime()) > 0 )
        {
            result = 1;
        }
        else
        {
            result = 0;
        }
        return result;
    }
}