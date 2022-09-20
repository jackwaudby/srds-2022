package event;

public class EpochTimeoutEvent extends AbstractEvent
{

    private final int epoch;

    public EpochTimeoutEvent( double eventTime, EventType eventTypeEnum, int epoch )
    {
        super( eventTime, eventTypeEnum );
        this.epoch = epoch;
    }

    public int getEpoch()
    {
        return epoch;
    }
}
