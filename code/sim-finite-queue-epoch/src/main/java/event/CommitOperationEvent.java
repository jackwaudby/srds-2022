package event;

public class CommitOperationEvent extends AbstractEvent
{
    private final int epoch;

    public CommitOperationEvent( double eventTime, EventType eventTypeEnum, int epoch )
    {
        super( eventTime, eventTypeEnum );
        this.epoch = epoch;
    }

    public int getEpoch()
    {
        return epoch;
    }
}
