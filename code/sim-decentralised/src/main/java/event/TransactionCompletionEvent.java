package event;

public class TransactionCompletionEvent extends AbstractEvent {
    private final int nodeId;

    public TransactionCompletionEvent(double eventTime, EventType eventTypeEnum, int nodeId) {
        super(eventTime, eventTypeEnum);
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }
}

