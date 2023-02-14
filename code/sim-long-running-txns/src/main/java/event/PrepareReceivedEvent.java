package event;

import state.Dependency;

import java.util.Set;

public class PrepareReceivedEvent extends AbstractEvent {
    private final int senderId;
    private final int receiverId;
    private final int senderEpoch;

    private final Set<Dependency> dependencies;

    public PrepareReceivedEvent(double eventTime, EventType eventTypeEnum, int senderId, int receiverId, int senderEpoch, Set<Dependency> dependencies) {
        super(eventTime, eventTypeEnum);
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.senderEpoch = senderEpoch;
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

    public int getSenderEpoch() {
        return senderEpoch;
    }
}

