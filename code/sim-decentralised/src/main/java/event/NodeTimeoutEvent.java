package event;

public class NodeTimeoutEvent extends AbstractEvent
{
    private final int nodeId;
    private final int epoch;

    public NodeTimeoutEvent(double eventTime, EventType eventTypeEnum, int nodeId, int epoch )
    {
        super( eventTime, eventTypeEnum );
        this.nodeId = nodeId;
        this.epoch = epoch;
    }

    public int getEpoch()
    {
        return epoch;
    }

    public int getNodeId() {
        return nodeId;
    }
}
