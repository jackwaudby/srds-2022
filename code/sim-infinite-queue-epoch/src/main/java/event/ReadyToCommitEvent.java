package event;

public class ReadyToCommitEvent extends AbstractEvent
{
    private final int epoch;

    public ReadyToCommitEvent( double eventTime, EventType eventTypeEnum, int epoch )
    {
        super( eventTime, eventTypeEnum );
        this.epoch = epoch;
    }

    public int getEpoch()
    {
        return epoch;
    }
}

