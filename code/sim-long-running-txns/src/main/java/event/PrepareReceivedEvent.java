package event;

public class PrepareReceivedEvent extends AbstractEvent {
    private final int senderId;

    private final int receiverId;
    private final int senderEpoch;

    public PrepareReceivedEvent(double eventTime, EventType eventTypeEnum, int senderId, int receiverId, int senderEpoch) {
        super(eventTime, eventTypeEnum);
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.senderEpoch = senderEpoch;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getReceiverId() {
        return receiverId;
    }
}

