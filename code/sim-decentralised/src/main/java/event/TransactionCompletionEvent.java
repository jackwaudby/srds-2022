package event;

public class TransactionCompletionEvent extends AbstractEvent {
    private final int nodeId;
    private final int epochStartedIn;

    public TransactionCompletionEvent(double eventTime, EventType eventTypeEnum, int nodeId, int epochStartedIn) {
        super(eventTime, eventTypeEnum);
        this.nodeId = nodeId;
        this.epochStartedIn = epochStartedIn;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getEpochTransactionStartedIn() {
        return epochStartedIn;
    }
}

