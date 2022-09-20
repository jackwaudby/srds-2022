package event;

public class ReadyToCommitEvent extends AbstractEvent
{

    public ReadyToCommitEvent( double eventTime, EventType eventTypeEnum )
    {
        super( eventTime, eventTypeEnum );
    }
}

