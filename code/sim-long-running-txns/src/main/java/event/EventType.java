package event;

public enum EventType
{
    EPOCH_TIMEOUT,
    TRANSACTION_COMPLETED,
    READY_TO_COMMIT,
    COMMIT_COMPLETED,
    ABORT_COMPLETED,
}
