package event;

public class DistributedTransactionEvent extends AbstractEvent
{
    private final int nodeId;

    public DistributedTransactionEvent( double eventTime, EventType eventTypeEnum, int nodeId )
    {
        super( eventTime, eventTypeEnum );
        this.nodeId = nodeId;
    }

    public int getNodeId()
    {
        return nodeId;
    }
}
