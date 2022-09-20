package event;

public class EpochTimeoutEvent extends AbstractEvent
{

    public EpochTimeoutEvent( double eventTime, EventType eventTypeEnum )
    {
        super( eventTime, eventTypeEnum );
    }
}
