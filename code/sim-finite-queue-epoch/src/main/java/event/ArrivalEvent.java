package event;

public class ArrivalEvent extends AbstractEvent
{
    public ArrivalEvent( double eventTime, EventType eventTypeEnum )
    {
        super( eventTime, eventTypeEnum );
    }
}