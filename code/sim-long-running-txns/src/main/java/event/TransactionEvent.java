package event;

public class TransactionEvent extends AbstractEvent
{
    private final int nodeId;
    private final int epoch;

    public TransactionEvent( double eventTime, EventType eventTypeEnum, int nodeId, int epoch )
    {
        super( eventTime, eventTypeEnum );
        this.nodeId = nodeId;
        this.epoch = epoch;
    }

    public int getNodeId()
    {
        return nodeId;
    }

    public int getEpoch()
    {
        return epoch;
    }
}
