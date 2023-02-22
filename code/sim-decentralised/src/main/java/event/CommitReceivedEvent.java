package event;

public class CommitReceivedEvent extends AbstractEvent
{
    private final int senderId;
    private final int receiverId;
    private final int epoch;

    public CommitReceivedEvent(double eventTime, EventType eventTypeEnum, int senderId, int receiverId, int epoch )
    {
        super( eventTime, eventTypeEnum );
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.epoch = epoch;
    }

    public int getEpoch()
    {
        return epoch;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getReceiverId() {
        return receiverId;
    }
}
