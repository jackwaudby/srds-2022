package event;

public class AbortOperationEvent extends AbstractEvent
{
    private final int epoch;

    public AbortOperationEvent( double eventTime, EventType eventTypeEnum, int epoch )
    {
        super( eventTime, eventTypeEnum );
        this.epoch = epoch;
    }

    public int getEpoch()
    {
        return epoch;
    }
}

