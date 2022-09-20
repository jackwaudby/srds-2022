package state;

public enum EpochState
{
    // processing job
    PROCESSING,

    // waiting for nodes to finish their current job
    WAITING,
}
