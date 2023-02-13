package event;

import state.Dependency;

import java.util.Set;

public class PrepareAckReceivedEvent extends AbstractEvent {
    private final int senderId;
    private final int receiverId;
    private final Set<Dependency> dependencies;

    public PrepareAckReceivedEvent(double eventTime, EventType eventTypeEnum, int senderId, int receiverId, Set<Dependency> dependencies) {
        super(eventTime, eventTypeEnum);
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.dependencies = dependencies;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getReceiverId() {
        return receiverId;
    }

    public Set<Dependency> getDependencies() {
        return dependencies;
    }
}